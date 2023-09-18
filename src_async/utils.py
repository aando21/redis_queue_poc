import aioredis
import src_async.config as config
import json


def redis_db():
    db = aioredis.from_url(
        f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/{config.REDIS_DB_NUMBER}",
        )
    return db


async def redis_task_queue_push(db, message):
    await db.lpush(config.REDIS_TASK_QUEUE_NAME, message)


async def redis_task_queue_pop(db):
    if await db.llen(config.REDIS_TASK_QUEUE_NAME) > 0:
        message = await db.rpop(config.REDIS_TASK_QUEUE_NAME)
        return message


# The following function is not used in the code since we are using the hash (dictionary) to store the results
async def redis_result_queue_pop(db):
    if await db.llen(config.REDIS_RESULT_QUEUE_NAME) > 0:
        message = await db.rpop(config.REDIS_RESULT_QUEUE_NAME)
        return message


async def redis_result_hash_pop(db, key):
    message = await db.hget(config.REDIS_RESULT_HASH_NAME, key)
    if message:
        json.loads(message.decode('utf-8'))
        await db.hdel(config.REDIS_RESULT_HASH_NAME, key)
    return message


async def redis_result_hash_push(db, key, message):
    await db.hset(config.REDIS_RESULT_HASH_NAME, key, message)


