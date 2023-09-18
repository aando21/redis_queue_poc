import json
import logging
import asyncio

import redis

from src.utils import redis_task_queue_pop, redis_result_hash_push, redis_db


class Worker:
    def __init__(self, db: redis.Redis):
        self.db = db

    async def run_main(self):
        while True:
            message = redis_task_queue_pop(self.db)
            if message:
                message = json.loads(message)
                logging.warning(f"Got message: {message}")
                redis_result_hash_push(self.db, message["id"], "working in progress")
                module, function = message["task"].rsplit(".", 1)
                module = __import__(module, fromlist=[function])
                if message.get("kwargs"):
                    result = await getattr(module, function)(
                        task_id=message["id"], **message.get("kwargs")
                    )
                else:
                    result = await getattr(module, function)(task_id=message["id"])
                await redis_result_hash_push(self.db, message["id"], json.dumps(result))
                redis_result_hash_push(self.db, message["id"], json.dumps(result))
            else:
                # Unblocks the startup phase of the FastAPI app
                await asyncio.sleep(1e-10)


if __name__ == "__main__":
    db = redis_db()
    worker = Worker(db)
    asyncio.run(worker.run_main())
