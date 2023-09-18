import aioredis
import src_async.config as config
import json
from typing import Union


def redis_db() -> aioredis.Redis:
    """
    This function creates a connection to a redis database
    Returns:
        db: redis database
    """
    db = aioredis.from_url(
        f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/{config.REDIS_DB_NUMBER}",
    )
    return db


async def redis_task_queue_push(db, message: str):
    """
    This function pushes the task to the task queue
    Args:
        db: redis database
        message: task to be pushed to the task queue
    """
    await db.lpush(config.REDIS_TASK_QUEUE_NAME, message)


async def redis_task_queue_pop(db) -> Union[str, None]:
    """
    This function pops the task from the task queue
    Args:
        db: redis database

    Returns:
        message: task to be popped from the task queue
    """
    if await db.llen(config.REDIS_TASK_QUEUE_NAME) > 0:
        message = await db.rpop(config.REDIS_TASK_QUEUE_NAME)
        return message


# The following function is not used in the code since we are using the hash (dictionary) to store the results
async def redis_result_queue_pop(db) -> Union[str, None]:
    """
    This function pops the result from the result queue
    Args:
        db: redis database

    Returns:
        message: result of the task
    """
    if await db.llen(config.REDIS_RESULT_QUEUE_NAME) > 0:
        message = await db.rpop(config.REDIS_RESULT_QUEUE_NAME)
        return message


async def redis_result_hash_pop(db, key: str) -> Union[bytes, None]:
    """
    This function "pops" the result from the hash (dictionary). To do so, it first gets the element and deletes the key
    from the hash (dictionary)
    Args:
        db: redis database
        key: uuid of the task

    Returns:
        message: result of the task
    """
    message = await db.hget(config.REDIS_RESULT_HASH_NAME, key)
    if message:
        json.loads(message.decode("utf-8"))
        await db.hdel(config.REDIS_RESULT_HASH_NAME, key)
    return message


async def redis_result_hash_push(db, key: str, message: str):
    """
    This function pushes the result to the result hash (dictionary)
    Args:
        db: redis database
        key: uuid of the task
        message: result of the task
    """
    await db.hset(config.REDIS_RESULT_HASH_NAME, key, message)
