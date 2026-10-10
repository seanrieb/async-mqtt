import asyncio
from async_mqtt.async_dns import Resolver

async def main():
    dns = Resolver("1.1.1.1")
    print(await dns.resolve("example.com", "A"))
    print(await dns.resolve("2001:db8::1", "AAAA"))

asyncio.run(main())
