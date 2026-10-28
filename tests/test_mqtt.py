import asyncio
import struct
import pytest
from async_mqtt import MQTTClient
from async_mqtt.async_mqtt import _remaining_length, _mqtt_string, _maybe_await

def test_length_encoding():
    assert _remaining_length(0) == b"\x00"
    assert _remaining_length(127) == b"\x7f"
    assert _remaining_length(128) == b"\x80\x01"
    assert _mqtt_string("abc") == b"\x00\x03abc"
    with pytest.raises(ValueError):
        _remaining_length(-1)

def test_connect_packet_without_will():
    client = MQTTClient("127.0.0.1", client_id="unit")
    packet = client._connect_packet()
    assert packet[0] == 0x10
    assert b"MQTT" in packet
    assert b"unit" in packet
    assert packet[9] & 0x04 == 0  # will flag off by default

def test_connect_packet_with_will():
    client = MQTTClient("127.0.0.1", client_id="unit")
    client.set_last_will("status", "offline", qos=1)
    packet = client._connect_packet()
    assert b"status" in packet and b"offline" in packet
    assert packet[9] & 0x04

def test_async_and_sync_callback():
    async def sample():
        return "async"
    assert asyncio.run(_maybe_await(sample())) == "async"
    assert asyncio.run(_maybe_await("sync")) == "sync"


class FakeWriter:
    def __init__(self):
        self.packets = []
        self.closed = False

    def write(self, packet):
        self.packets.append(packet)

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        self.closed = True

    async def wait_closed(self):
        await asyncio.sleep(0)


def test_pingresp_wakes_waiter():
    async def exercise():
        client = MQTTClient("localhost")
        client._ping_outstanding = True
        await client._handle_packet(0xD0, b"")
        assert client._ping_response.is_set()
    asyncio.run(exercise())


def test_unsolicited_pingresp_does_not_satisfy_next_ping():
    async def exercise():
        client = MQTTClient("localhost")
        await client._handle_packet(0xD0, b"")
        assert not client._ping_response.is_set()
    asyncio.run(exercise())


def test_missing_pingresp_disconnects_and_notifies():
    async def exercise():
        disconnected = []
        async def callback(client, error):
            disconnected.append(error)
        client = MQTTClient("localhost", keepalive=2, response_timeout=0.03,
                            on_disconnect=callback)
        writer = FakeWriter()
        client._writer = writer
        client._closed.clear()
        await client._keepalive_loop()
        assert writer.packets == [b"\\xc0\\x00"]
        assert writer.closed
        assert not client.connected
        assert client._closed.is_set()
        assert len(disconnected) == 1
        assert "PINGRESP" in str(disconnected[0])
    asyncio.run(exercise())


def test_immediate_pingresp_during_send_succeeds():
    async def exercise():
        client = MQTTClient("localhost", keepalive=2, response_timeout=0.03)
        writer = FakeWriter()
        client._writer = writer
        client._closed.clear()
        real_send = client._send
        async def send_and_reply(packet_type_flags, body=b""):
            await real_send(packet_type_flags, body)
            await client._handle_packet(0xD0, b"")
        client._send = send_and_reply
        task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(1.07)
        assert client.connected
        assert writer.packets == [b"\\xc0\\x00"]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await client._close_transport()
    asyncio.run(exercise())


def test_stalled_wait_closed_is_bounded():
    async def exercise():
        class StalledWriter(FakeWriter):
            async def wait_closed(self):
                await asyncio.Event().wait()
        client = MQTTClient("localhost", response_timeout=0.02)
        writer = StalledWriter()
        client._writer = writer
        client._closed.clear()
        await asyncio.wait_for(client._close_transport(), 0.3)
        assert writer.closed and client._closed.is_set()
    asyncio.run(exercise())
