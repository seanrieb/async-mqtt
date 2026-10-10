"""Small asynchronous DNS client for MicroPython.

This module performs DNS lookups without calling the blocking
``socket.getaddrinfo()``.  It uses one nonblocking UDP socket and yields to the
MicroPython asyncio scheduler while waiting for a reply.

Example::

    import asyncio
    from async_dns import Resolver

    async def main():
        dns = Resolver("192.168.1.1")
        print(await dns.resolve("example.com"))
        print(await dns.resolve("example.com", "AAAA"))

    asyncio.run(main())
"""

# Author: Sean Rieb
# License: MIT

import asyncio
import socket
import struct
import time

try:
    import urandom as _random
except ImportError:
    import random as _random


_TYPES = {"A": 1, "AAAA": 28}


class DNSError(Exception):
    """A DNS query failed or returned an invalid response."""


def _is_ipv4(value):
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for part in parts:
        if not part or not part.isdigit():
            return False
        number = int(part)
        if number > 255:
            return False
    return True


def _is_ipv6(value):
    """Validate an IPv6 literal without relying on socket.inet_pton()."""
    if ":" not in value or value.count("::") > 1:
        return False

    compressed = "::" in value
    if compressed:
        left, right = value.split("::")
        parts = ([] if not left else left.split(":"))
        parts += ([] if not right else right.split(":"))
    else:
        parts = value.split(":")

    words = 0
    for index, part in enumerate(parts):
        if not part:
            return False
        if "." in part:
            # An embedded IPv4 address is allowed only in the final position.
            if index != len(parts) - 1 or not _is_ipv4(part):
                return False
            words += 2
        else:
            if len(part) > 4:
                return False
            try:
                int(part, 16)
            except ValueError:
                return False
            words += 1

    # :: must replace at least one 16-bit word.
    return words < 8 if compressed else words == 8


def _ip_literal(host):
    """Return an unbracketed IP literal, or None if *host* is a name."""
    value = host
    if len(value) >= 2 and value[0] == "[" and value[-1] == "]":
        value = value[1:-1]
    if _is_ipv4(value) or _is_ipv6(value):
        return value
    return None


def _transaction_id():
    try:
        return struct.unpack("!H", _random.urandom(2))[0]
    except AttributeError:
        return _random.getrandbits(16)


def _encode_name(host):
    host = host.rstrip(".")
    if not host:
        raise ValueError("hostname is empty")

    result = bytearray()
    for label in host.split("."):
        encoded = label.encode("utf-8")
        size = len(encoded)
        if not size or size > 63:
            raise ValueError("invalid DNS label")
        result.append(size)
        result.extend(encoded)
    result.append(0)

    if len(result) > 255:
        raise ValueError("hostname is too long")
    return result


def _make_query(host, qtype, query_id):
    # RD (recursion desired), one question.
    packet = bytearray(struct.pack("!HHHHHH", query_id, 0x0100, 1, 0, 0, 0))
    packet.extend(_encode_name(host))
    packet.extend(struct.pack("!HH", qtype, 1))  # QTYPE, IN
    return packet


def _skip_name(packet, offset):
    """Return the offset immediately following an encoded DNS name."""
    length = len(packet)
    while True:
        if offset >= length:
            raise DNSError("truncated DNS name")
        size = packet[offset]
        if size == 0:
            return offset + 1
        if size & 0xC0 == 0xC0:
            if offset + 1 >= length:
                raise DNSError("truncated DNS pointer")
            return offset + 2
        if size & 0xC0:
            raise DNSError("invalid DNS label")
        offset += size + 1


def _format_ipv6(data):
    words = struct.unpack("!8H", data)

    # Find the longest sequence of zero words for :: compression.
    best_start = -1
    best_length = 0
    start = -1
    for i in range(9):
        zero = i < 8 and words[i] == 0
        if zero and start < 0:
            start = i
        elif not zero and start >= 0:
            run = i - start
            if run > best_length and run >= 2:
                best_start, best_length = start, run
            start = -1

    if best_start < 0:
        return ":".join("%x" % word for word in words)

    left = ":".join("%x" % words[i] for i in range(best_start))
    right_start = best_start + best_length
    right = ":".join("%x" % words[i] for i in range(right_start, 8))
    if left and right:
        return left + "::" + right
    if left:
        return left + "::"
    if right:
        return "::" + right
    return "::"


def _parse_response(packet, query_id, wanted_type):
    if len(packet) < 12:
        raise DNSError("short DNS response")

    response_id, flags, questions, answers, _, _ = struct.unpack(
        "!HHHHHH", packet[:12]
    )
    if response_id != query_id:
        return None
    if not flags & 0x8000:
        return None
    if flags & 0x0200:
        raise DNSError("DNS response was truncated")

    rcode = flags & 0x000F
    if rcode:
        names = {
            1: "format error",
            2: "server failure",
            3: "name does not exist",
            4: "not implemented",
            5: "query refused",
        }
        raise DNSError(names.get(rcode, "DNS error %d" % rcode))

    offset = 12
    for _ in range(questions):
        offset = _skip_name(packet, offset)
        if offset + 4 > len(packet):
            raise DNSError("truncated DNS question")
        offset += 4

    result = []
    for _ in range(answers):
        offset = _skip_name(packet, offset)
        if offset + 10 > len(packet):
            raise DNSError("truncated DNS answer")

        answer_type, answer_class, _, data_length = struct.unpack(
            "!HHIH", packet[offset : offset + 10]
        )
        offset += 10
        end = offset + data_length
        if end > len(packet):
            raise DNSError("truncated DNS data")

        if answer_class == 1 and answer_type == wanted_type:
            data = packet[offset:end]
            if answer_type == 1 and data_length == 4:
                result.append("%d.%d.%d.%d" % tuple(data))
            elif answer_type == 28 and data_length == 16:
                result.append(_format_ipv6(data))
        offset = end

    return result


class Resolver:
    """Lightweight reusable asynchronous DNS resolver.

    Args:
        server: DNS server IPv4 address. Usually the LAN router or a public DNS
            resolver. A numeric address avoids the lookup this class replaces.
        port: DNS UDP port.
        timeout_ms: Time allowed for each attempt.
        retries: Total number of query attempts.
        poll_ms: Scheduler yield interval while waiting for UDP data.
    """

    def __init__(
        self,
        server="1.1.1.1",
        port=53,
        timeout_ms=2000,
        retries=2,
        poll_ms=10,
    ):
        self.server = server
        self.port = port
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.poll_ms = poll_ms

    async def resolve(self, host, qtype="A"):
        """Resolve *host* and return a list of address strings.

        ``qtype`` may be ``"A"``, ``"AAAA"``, 1, or 28.  An empty list means
        the DNS server returned a successful response with no matching records.
        A timeout or DNS failure raises :class:`DNSError`.
        """
        if isinstance(qtype, str):
            try:
                qtype = _TYPES[qtype.upper()]
            except KeyError:
                raise ValueError("qtype must be A or AAAA")
        if qtype not in (1, 28):
            raise ValueError("qtype must be A (1) or AAAA (28)")

        # IP literals already are resolved.  This occurs before allocating a
        # query or socket and does not yield to the event loop.
        literal = _ip_literal(host)
        if literal is not None:
            return [literal]

        query_id = _transaction_id()
        query = _make_query(host, qtype, query_id)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)

        try:
            for _ in range(self.retries):
                sock.sendto(query, (self.server, self.port))
                started = time.ticks_ms()

                while time.ticks_diff(time.ticks_ms(), started) < self.timeout_ms:
                    try:
                        packet, source = sock.recvfrom(512)
                    except OSError:
                        await asyncio.sleep_ms(self.poll_ms)
                        continue

                    if source[0] != self.server or source[1] != self.port:
                        continue
                    parsed = _parse_response(packet, query_id, qtype)
                    if parsed is not None:
                        return parsed

            raise DNSError("DNS lookup timed out")
        finally:
            sock.close()


async def resolve(host, server="1.1.1.1", qtype="A", **kwargs):
    """Convenience wrapper for a single query."""
    return await Resolver(server, **kwargs).resolve(host, qtype)
