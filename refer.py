import pymongo
from loguru import logger

client = pymongo.MongoClient()
db = client['bybit']
collection = db['USDT']

# 2021-05-30T05:59:18.000Z 以降のドキュメントを5件取得する
for doc in collection.find({'timestamp': {'$gte': '2021-05-30T05:59:18.000Z'}}, projection={'_id': False}).limit(5):
    logger.info(doc)

print('-' * 80)

# 最新のレコードを降順で1件取得する
doc = collection.find_one(sort=[('_id', pymongo.DESCENDING)], projection={'_id': False})
logger.info(doc)