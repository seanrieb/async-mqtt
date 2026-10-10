import asyncio
import struct
from async_mqtt.async_dns import Resolver, _is_ipv4, _is_ipv6, _make_query, _parse_response, DNSError

def test_literals():
    resolver = Resolver()
    assert asyncio.run(resolver.resolve("192.168.1.1")) == ["192.168.1.1"]
    assert asyncio.run(resolver.resolve("[2001:db8::1]", "AAAA")) == ["2001:db8::1"]
    assert _is_ipv4("255.0.12.2")
    assert not _is_ipv4("999.0.0.1")
    assert _is_ipv6("::1")
    assert not _is_ipv6("1:2:3")

def test_dns_response():
    question = _make_query("example.com", 1, 42)[12:]
    answer = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 60, 4) + bytes((1, 2, 3, 4))
    response = struct.pack("!HHHHHH", 42, 0x8180, 1, 1, 0, 0) + question + answer
    assert _parse_response(response, 42, 1) == ["1.2.3.4"]
    assert _parse_response(response, 43, 1) is None
