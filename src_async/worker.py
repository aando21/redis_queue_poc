import json
import logging
import asyncio

from src_async.redis_task import perform_task
from src_async.utils import redis_task_queue_pop, redis_result_hash_push, redis_db


class Worker:
    def __init__(self, db):
        self.db = db

    async def run_main(self):
        while True:
            message = await redis_task_queue_pop(self.db)
            if message:
                message = json.loads(message)
                logging.warning(f'Got message: {message}')
                await redis_result_hash_push(self.db, message['id'], 'working in progress')
                result = await perform_task(message['id'])
                await redis_result_hash_push(self.db, message['id'], json.dumps(result))
            else:
                # Unblocks the startup phase of the FastAPI app
                await asyncio.sleep(1e-10)


if __name__ == '__main__':
    db = redis_db()
    worker = Worker(db)
    asyncio.run(worker.run_main())
