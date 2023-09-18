import redis
import src.config as config
import json


def redis_db():
    db = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        # password=config.REDIS_PASSWORD,
        db=config.REDIS_DB_NUMBER,
    )
    db.ping()
    return db


def redis_task_queue_push(db: redis.Redis, message):
    db.lpush(config.REDIS_TASK_QUEUE_NAME, message)


def redis_task_queue_pop(db):
    if db.llen(config.REDIS_TASK_QUEUE_NAME) > 0:
        message = db.rpop(config.REDIS_TASK_QUEUE_NAME)
        return message


# The following function is not used in the code since we are using the hash (dictionary) to store the results
def redis_result_queue_pop(db):
    if db.llen(config.REDIS_RESULT_QUEUE_NAME) > 0:
        message = db.rpop(config.REDIS_RESULT_QUEUE_NAME)
        return message


async def redis_result_hash_pop(db, key):
    message = db.hget(config.REDIS_RESULT_HASH_NAME, key)
    if message:
        json.loads(message.decode("utf-8"))
        db.hdel(config.REDIS_RESULT_HASH_NAME, key)
    return message


def redis_result_hash_push(db, key, message):
    db.hset(config.REDIS_RESULT_HASH_NAME, key, message)
