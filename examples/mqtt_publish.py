import asyncio
from async_mqtt import MQTTClient

async def main():
    client = MQTTClient("192.168.1.10", client_id="example-publisher")
    await client.connect()
    try:
        await client.publish("example/status", "online", qos=1, retain=True)
    finally:
        await client.disconnect()

asyncio.run(main())
