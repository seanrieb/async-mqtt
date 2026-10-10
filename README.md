# async-mqtt

Lightweight MicroPython **MQTT 3.1.1** client using asyncio streams, bundled with a separately reusable, nonblocking DNS resolver.

## Features

- MQTT QoS 0/1 publish and subscribe; retained messages; optional username/password and TLS
- Last Will, keepalive and reconnect loop with exponential backoff
- Synchronous or asynchronous callbacks
- Standalone async `A` and `AAAA` DNS lookups, with immediate return for IP literals
- Source deployment via MicroPython `mip` / `mpremote`

**Not supported:** MQTT QoS 2, MQTT 5. This is an initial development release; actual firmware/board integration testing is still recommended.

## Install on MicroPython

Once you publish this repository to GitHub, replace `YOUR_USER` below with your account or organization:

```sh
mpremote mip install github:YOUR_USER/async-mqtt
```

Alternatively, from a network-enabled MicroPython REPL:

```python
import mip
mip.install("github:YOUR_USER/async-mqtt")
```

For reproducibility after a GitHub release, pin the installation to a tag such as `v0.1.0` using the installer's version parameter.

## Use MQTT

```python
import asyncio
from async_mqtt import MQTTClient

async def on_message(client, topic, payload, retain, qos):
    print(topic, payload)

async def main():
    client = MQTTClient("192.168.1.10", client_id="pico-2w", on_message=on_message)
    await client.subscribe("example/#", qos=0)
    await client.run_forever()

asyncio.run(main())
```

**Important:** `subscribe()` stores topics for the next connection. It does not open a connection on its own. You may also use `await client.connect()` then `await client.publish(...)` and `await client.disconnect()`.

## Use DNS independently

```python
from async_mqtt.async_dns import Resolver, resolve

dns = Resolver("192.168.1.1")
addresses = await dns.resolve("example.com", "A")
# Or: addresses = await resolve("example.com", server="192.168.1.1")
```

To use explicit DNS with MQTT:

```python
from async_mqtt import MQTTClient, Resolver
resolver = Resolver("192.168.1.1")
client = MQTTClient("broker.example.org", resolve=resolver.resolve)
```

The DNS utility is separately importable, but installed as part of the same package. It uses UDP and needs a configured DNS server; by default, it uses `1.1.1.1`.

## Development (macOS/Linux, CPython)

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e . pytest
python -m pytest -q
```

CPython tests exercise protocol helpers and simulated callbacks only; integration testing on MicroPython hardware and a real broker is a separate step.

## Layout

- `async_mqtt/async_mqtt.py` — MQTT client
- `async_mqtt/async_dns.py` — standalone asynchronous DNS resolver
- `async_mqtt/__init__.py` — convenient public imports
- `tests/` — automated unit tests
- `examples/` — MicroPython samples
- `package.json` — MicroPython installer manifest

## Release checklist

1. Confirm ownership and use of the MIT license; edit `LICENSE` copyright as needed.
2. Publish repository under the name `async-mqtt` (or update installation instructions to match).
3. Run CPython tests and test on your supported MicroPython boards.
4. Create a GitHub release/tag after hardware testing.

## License

MIT (see `LICENSE`).
