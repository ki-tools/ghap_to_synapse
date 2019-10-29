import asyncio


class AioManager:

    @classmethod
    def start(cls, func, **kwargs):
        asyncio.run(cls._start_async(func, **kwargs))

    @classmethod
    async def _start_async(cls, func, **kwargs):
        await func(**kwargs)
