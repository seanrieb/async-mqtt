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
