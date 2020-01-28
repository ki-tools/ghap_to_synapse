# Copyright 2017-present, Bill & Melinda Gates Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import aiohttp


class AioManager:
    AIOSESSION = None

    @classmethod
    def start(cls, func, **kwargs):
        asyncio.run(cls._start_async(func, **kwargs))

    @classmethod
    async def _start_async(cls, func, **kwargs):
        try:
            cls.AIOSESSION = aiohttp.ClientSession(raise_for_status=True)
            await func(**kwargs)
        finally:
            if cls.AIOSESSION:
                await cls.AIOSESSION.close()
