import asyncio
from datetime import datetime
import src_async.config as config


async def perform_task(task_id: str, extra_time: int) -> dict:
    # Simulate a time-consuming task (replace with your actual task)
    await asyncio.sleep(config.TIME_OF_TASK_EXECUTION)
    await asyncio.sleep(extra_time)
    return {"task_id": task_id, "result": "Task completed at " + str(datetime.now())}
