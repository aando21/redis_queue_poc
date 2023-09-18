import json
import logging
import asyncio

from src_async.utils import redis_task_queue_pop, redis_result_hash_push, redis_db


class Worker:
    """
    This class is responsible for running the tasks in the background.
    """

    def __init__(self, db):
        self.db = db

    async def run_main(self):
        """
        This function runs the tasks in the background.
        """
        while True:
            message = await redis_task_queue_pop(self.db)
            if message:
                message = json.loads(message)
                logging.warning(f"Got message: {message}")
                await redis_result_hash_push(
                    self.db, message["id"], "working in progress"
                )
                module, function = message["task"].rsplit(".", 1)
                module = __import__(module, fromlist=[function])
                if message.get("kwargs"):
                    result = await getattr(module, function)(
                        task_id=message["id"], **message.get("kwargs")
                    )
                else:
                    result = await getattr(module, function)(task_id=message["id"])
                await redis_result_hash_push(self.db, message["id"], json.dumps(result))
            else:
                # Unblocks the startup phase of the FastAPI app
                await asyncio.sleep(1e-10)


if __name__ == "__main__":
    db = redis_db()
    worker = Worker(db)
    asyncio.run(worker.run_main())
