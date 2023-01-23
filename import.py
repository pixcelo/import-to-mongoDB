import asyncio
from typing import Any, Dict

import motor.motor_asyncio
import pybotters

class BybitMongoDB:
    def __init__(self):
        """
        インスタンス作成時にホスト、ポートを指定することも可
        e.g.
        client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
        """
        client = motor.motor_asyncio.AsyncIOMotorClient()
        db = client['bybit']
        self.collection = db['USDT']
        self.queue = asyncio.Queue()
        asyncio.create_task(self.consumer())

    def onmessage(self, msg: Dict[str, Any], ws):
        if 'topic' in msg:
            topic: str = msg['topic']
            if topic.startswith('trade'):
                self.queue.put_nowait(msg['data'])

    async def consumer(self):
        while True:
            data = await self.queue.get()
            await self.collection.insert_many(data)


async def main():
    async with pybotters.Client() as client:
        db = BybitMongoDB()
        wstask = await client.ws_connect(
            # inverse と USDT はWebSocketの購読URLが異なる
            'wss://stream.bybit.com/realtime_public',
            send_json={'op': 'subscribe', 'args': ['trade.BTCUSDT']},
            hdlr_json=db.onmessage,
        )
        await wstask

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass