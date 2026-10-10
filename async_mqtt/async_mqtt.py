"""Small MQTT 3.1.1 client for MicroPython using asyncio TCP streams.

Supported features:
  * asyncio.open_connection() TCP transport (optional TLS)
  * Username/password and Last Will
  * QoS 0 and QoS 1 publish
  * QoS 0 and QoS 1 subscribe
  * Retained messages
  * Keepalive ping and automatic reconnect
  * Synchronous or async message/connection callbacks

The module intentionally does not implement MQTT QoS 2 or MQTT 5.

Example:

    import asyncio
    from async_mqtt import MQTTClient

    async def on_message(client, topic, payload, retain, qos):
        print(topic, payload)

    async def on_connect(client):
        print("connected")

    async def main():
        client = MQTTClient(
            "192.168.1.10",
            client_id="pico-2w",
            username="device",
            password="secret",
            on_message=on_message,
            on_connect=on_connect,
        )
        await client.run_forever()

    asyncio.run(main())
"""

# Author: Sean Rieb
# License: MIT

try:
    import uasyncio as asyncio
except ImportError:
    import asyncio
import struct
try:
    import micropython
except ImportError:
    class micropython:
        native = staticmethod(lambda fn: fn)

# Create a raw reference generator type since 'types' module doesn't exist
GeneratorType = type((x for x in []))


class MQTTError(Exception):
    pass


class MQTTConnectionError(MQTTError):
    pass


class MQTTProtocolError(MQTTError):
    pass

@micropython.native
def _as_bytes(value):
    if isinstance(value, bytes) or isinstance(value, memoryview):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode()


@micropython.native
def _mqtt_string(value):
    value = _as_bytes(value)
    if len(value) > 65535:
        raise ValueError("MQTT string is too long")
    return struct.pack("!H", len(value)) + value


@micropython.native
def _remaining_length(value):
    if value < 0 or value > 268435455:
        raise ValueError("invalid MQTT remaining length")
    encoded = bytearray()
    while True:
        digit = value & 0x7F
        value >>= 7
        if value:
            digit |= 0x80
        encoded.append(digit)
        if not value:
            return bytes(encoded)


async def _maybe_await(result):
    if hasattr(result, "__await__") or isinstance(result, GeneratorType):
        return await result
    return result


async def _wait_for(awaitable, timeout_s):
    """Use the timeout spelling supported by the installed asyncio build."""
    if hasattr(asyncio, "wait_for_ms"):
        return await asyncio.wait_for_ms(awaitable, int(timeout_s * 1000))
    return await asyncio.wait_for(awaitable, timeout_s)


class MQTTClient:
    """An asynchronous MQTT 3.1.1 client.

    Call ``connect()`` for a single connection, or ``run_forever()`` for an
    automatically reconnecting client. Subscriptions are remembered and are
    restored after reconnecting.
    """

    _CONNACK_ERRORS = (
        "connection accepted",
        "unacceptable protocol version",
        "client identifier rejected",
        "server unavailable",
        "bad username or password",
        "not authorized",
    )

    def __init__(
        self,
        host,
        port=1883,
        client_id=None,
        username=None,
        password=None,
        keepalive=60,
        clean_session=True,
        ssl=None,
        on_message=None,
        on_connect=None,
        on_disconnect=None,
        response_timeout=10,
        reconnect_ms_min=1000,
        reconnect_ms_max=60000,
        resolve=None,
    ):
        self.host = host
        self.port = port
        self.client_id = _as_bytes(client_id or "micropython")
        self.username = None if username is None else _as_bytes(username)
        self.password = None if password is None else _as_bytes(password)
        self.keepalive = int(keepalive)
        self.clean_session = bool(clean_session)
        self.ssl = ssl
        self.on_message = on_message
        self._legacy_callback = False
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.response_timeout = response_timeout
        self.reconnect_min = reconnect_ms_min
        self.reconnect_max = reconnect_ms_max
        self.resolve = resolve

        self._reader = None
        self._writer = None
        self._reader_task = None
        self._ping_task = None
        self._write_lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._closed.set()
        self._stopping = False
        self._packet_id = 0
        self._waiters = {}
        self._subscriptions = {}
        self._will = None

    @property
    def connected(self):
        return self._writer is not None and not self._closed.is_set()

    def set_callback(self, callback):
        """Compatibility helper: callback(topic, payload)."""
        self.on_message = callback
        self._legacy_callback = True

    def set_last_will(self, topic, payload, qos=0, retain=False):
        if qos not in (0, 1):
            raise ValueError("only QoS 0 and 1 are supported")
        self._will = (_as_bytes(topic), _as_bytes(payload), qos, bool(retain))

    def _next_packet_id(self):
        self._packet_id = (self._packet_id % 65535) + 1
        while self._packet_id in self._waiters:
            self._packet_id = (self._packet_id % 65535) + 1
        return self._packet_id

    async def _read_exactly(self, count):
        data = await self._reader.readexactly(count)
        if len(data) != count:
            raise MQTTConnectionError("connection closed")
        return data

    async def _read_packet(self):
        first = (await self._read_exactly(1))[0]
        multiplier = 1
        remaining = 0
        for _ in range(4):
            digit = (await self._read_exactly(1))[0]
            remaining += (digit & 0x7F) * multiplier
            if not (digit & 0x80):
                return first, await self._read_exactly(remaining)
            multiplier *= 128
        raise MQTTProtocolError("malformed remaining length")

    async def _send(self, packet_type_flags, body=b""):
        if self._writer is None:
            raise MQTTConnectionError("not connected")
        packet = bytes((packet_type_flags,)) + _remaining_length(len(body)) + body
        async with self._write_lock:
            self._writer.write(packet)
            drain = getattr(self._writer, "drain", None)
            if drain is not None:
                await drain()

    async def _send_parts(self, packet_type_flags, body_length, parts):
        if self._writer is None:
            raise MQTTConnectionError("not connected")
        fixed_header = bytes((packet_type_flags,))
        fixed_header += _remaining_length(body_length)
        async with self._write_lock:
            self._writer.write(fixed_header)
            for part in parts:
                if part:
                    self._writer.write(memoryview(part))
            drain = getattr(self._writer, "drain", None)
            if drain is not None:
                await drain()

    @micropython.native
    def _connect_packet(self):
        flags = 0x02 if self.clean_session else 0
        payload = _mqtt_string(self.client_id)

        if self._will is not None:
            topic, message, qos, retain = self._will
            flags |= 0x04 | (qos << 3)
            if retain:
                flags |= 0x20
            payload += _mqtt_string(topic) + _mqtt_string(message)
        if self.username is not None:
            flags |= 0x80
            payload += _mqtt_string(self.username)
        if self.password is not None:
            if self.username is None:
                raise ValueError("password requires a username")
            flags |= 0x40
            payload += _mqtt_string(self.password)

        variable = _mqtt_string(b"MQTT") + bytes((4, flags))
        variable += struct.pack("!H", self.keepalive)
        body = variable + payload
        return bytes((0x10,)) + _remaining_length(len(body)) + body

    async def connect(self):
        """Open TCP, complete MQTT CONNECT, then start background tasks."""
        if self.connected:
            return
        self._stopping = False
        self._closed.clear()
        broker = [self.host]
        if self.resolve is not None:
            broker = await self.resolve(self.host)
        try:
            kwargs = {}
            if self.ssl is not None:
                kwargs["ssl"] = self.ssl
            self._reader, self._writer = await asyncio.open_connection(
                broker[0], self.port, **kwargs
            )
            self._writer.write(self._connect_packet())
            drain = getattr(self._writer, "drain", None)
            if drain is not None:
                await drain()

            first, body = await _wait_for(self._read_packet(), self.response_timeout)
            if first != 0x20 or len(body) != 2:
                raise MQTTProtocolError("expected CONNACK")
            return_code = body[1]
            if return_code:
                if return_code < len(self._CONNACK_ERRORS):
                    reason = self._CONNACK_ERRORS[return_code]
                else:
                    reason = "unknown return code"
                raise MQTTConnectionError("CONNACK %d: %s" % (return_code, reason))

            self._reader_task = asyncio.create_task(self._reader_loop())
            if self.keepalive:
                self._ping_task = asyncio.create_task(self._keepalive_loop())

            # A clean session has no broker-side subscription state. Sending
            # these again is harmless for persistent sessions and deterministic.
            saved = tuple(self._subscriptions.items())
            for topic, qos in saved:
                await self._subscribe_packet(topic, qos)

            if self.on_connect is not None:
                await _maybe_await(self.on_connect(self))
        except Exception:
            await self._close_transport()
            raise

    async def disconnect(self):
        """Send MQTT DISCONNECT and prevent run_forever() reconnecting."""
        self._stopping = True
        if self.connected:
            try:
                await self._send(0xE0)
            except Exception:
                pass
        await self._close_transport()

    async def wait_closed(self):
        await self._closed.wait()

    async def _close_transport(self, error=None):
        was_open = self._writer is not None
        writer = self._writer
        self._reader = None
        self._writer = None

        current = None
        current_task = getattr(asyncio, "current_task", None)
        if current_task is not None:
            current = current_task()
        for task in (self._reader_task, self._ping_task):
            if task is not None and task is not current:
                task.cancel()
        self._reader_task = None
        self._ping_task = None

        for waiter in self._waiters.values():
            waiter["error"] = error or MQTTConnectionError("disconnected")
            waiter["event"].set()
        self._waiters.clear()

        if writer is not None:
            try:
                writer.close()
                wait_closed = getattr(writer, "wait_closed", None)
                if wait_closed is not None:
                    await wait_closed()
            except Exception:
                pass
        self._closed.set()
        if was_open and error is not None and self.on_disconnect is not None:
            await _maybe_await(self.on_disconnect(self, error))

    async def _reader_loop(self):
        error = None
        try:
            while self._writer is not None:
                first, body = await self._read_packet()
                await self._handle_packet(first, body)
        except Exception as exc:
            error = exc
        finally:
            if self._writer is not None:
                await self._close_transport(error)

    async def _keepalive_loop(self):
        try:
            while self._writer is not None:
                await asyncio.sleep(max(1, self.keepalive // 2))
                await self._send(0xC0)
        except Exception as exc:
            if self._writer is not None:
                await self._close_transport(exc)

    async def _handle_packet(self, first, body):
        packet_type = first >> 4

        if packet_type == 3:  # PUBLISH
            if len(body) < 2:
                raise MQTTProtocolError("short PUBLISH")
            topic_length = struct.unpack("!H", body[:2])[0]
            pos = 2
            end = pos + topic_length
            if end > len(body):
                raise MQTTProtocolError("bad PUBLISH topic length")
            topic = body[pos:end]
            pos = end
            qos = (first >> 1) & 0x03
            packet_id = None
            if qos:
                if qos == 2:
                    raise MQTTProtocolError("incoming QoS 2 is unsupported")
                if pos + 2 > len(body):
                    raise MQTTProtocolError("missing PUBLISH packet id")
                packet_id = struct.unpack("!H", body[pos:pos + 2])[0]
                pos += 2
            payload = body[pos:]
            retain = bool(first & 0x01)
            if self.on_message is not None:
                if self._legacy_callback:
                    await _maybe_await(self.on_message(topic, payload))
                else:
                    await _maybe_await(
                        self.on_message(self, topic, payload, retain, qos)
                    )
            if qos == 1:
                await self._send(0x40, struct.pack("!H", packet_id))
            return

        if packet_type in (4, 9, 11):  # PUBACK, SUBACK, or UNSUBACK
            if len(body) < 2:
                raise MQTTProtocolError("short acknowledgement")
            packet_id = struct.unpack("!H", body[:2])[0]
            waiter = self._waiters.get(packet_id)
            if waiter is not None:
                waiter["response"] = body[2:]
                waiter["event"].set()
            return

        if packet_type == 13:  # PINGRESP
            return

        if packet_type == 14:  # Server DISCONNECT (MQTT 5 brokers may send it)
            raise MQTTConnectionError("broker disconnected")

    async def _request(self, packet_type_flags, body, packet_id):
        waiter = {"event": asyncio.Event(), "response": None, "error": None}
        self._waiters[packet_id] = waiter
        try:
            await self._send(packet_type_flags, body)
            await _wait_for(waiter["event"].wait(), self.response_timeout)
            if waiter["error"] is not None:
                raise waiter["error"]
            return waiter["response"]
        finally:
            self._waiters.pop(packet_id, None)

    async def publish(self, topic, payload=b"", qos=0, retain=False):
        """Publish bytes/string data. QoS may be 0 or 1."""
        if qos not in (0, 1):
            raise ValueError("only QoS 0 and 1 are supported")
        topic = _as_bytes(topic)
        payload = _as_bytes(payload)
        flags = (qos << 1) | (1 if retain else 0)
        if qos == 0:
            if self.connected:
                topic_view = memoryview(topic)
                payload_view = memoryview(payload)
                topic_length = struct.pack("!H", len(topic_view))
                body_length = 2+len(topic_view)+len(payload_view)
                try:
                    await self._send_parts(
                        0x30 | flags,
                        body_length,
                        (topic_length, topic_view, payload_view),
                    )
                except OSError as exc:
                    print(exc)
                    if self._writer is not None:
                        await self._close_transport(exc)
            return
        body = _mqtt_string(topic)
        packet_id = self._next_packet_id()
        body += struct.pack("!H", packet_id) + payload
        await self._request(0x30 | flags, body, packet_id)

    async def _subscribe_packet(self, topic, qos):
        packet_id = self._next_packet_id()
        body = struct.pack("!H", packet_id) + _mqtt_string(topic) + bytes((qos,))
        response = await self._request(0x82, body, packet_id)
        if not response or response[0] == 0x80:
            raise MQTTProtocolError("subscription rejected")
        return response[0]

    async def subscribe(self, topic, qos=0):
        """Subscribe and remember the subscription for future reconnects."""
        if qos not in (0, 1):
            raise ValueError("only QoS 0 and 1 are supported")
        topic = _as_bytes(topic)
        self._subscriptions[topic] = qos
        if self.connected:
            return await self._subscribe_packet(topic, qos)

    async def unsubscribe(self, topic):
        topic = _as_bytes(topic)
        self._subscriptions.pop(topic, None)
        if not self.connected:
            return
        packet_id = self._next_packet_id()
        body = struct.pack("!H", packet_id) + _mqtt_string(topic)
        # UNSUBACK has the same packet-id waiter shape as PUBACK.
        await self._request(0xA2, body, packet_id)

    async def run_forever(self):
        """Connect, reconnect with bounded exponential backoff, and wait."""
        self._stopping = False
        delay = self.reconnect_min
        while not self._stopping:
            try:
                await self.connect()
                delay = self.reconnect_min
                await self.wait_closed()
            except Exception:
                # Connection/setup failures are retried here. If a transport
                # existed, _close_transport() already invoked on_disconnect.
                pass
            if not self._stopping:
                await asyncio.sleep_ms(delay)
                delay = min(delay * 2, self.reconnect_max)
