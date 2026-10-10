import asyncio
from async_mqtt import MQTTClient

async def on_message(client, topic, payload, retain, qos):
    print(topic, payload, retain, qos)

async def main():
    client = MQTTClient("192.168.1.10", client_id="example-subscriber", on_message=on_message)
    await client.subscribe("example/#")
    await client.run_forever()

asyncio.run(main())
