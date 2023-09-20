import asyncio
import time
from datetime import datetime
import src.config as config


def perform_task(task_id: str, extra_time: int) -> dict:
    # Simulate a time-consuming task (replace with your actual task)
    time.sleep(config.TIME_OF_TASK_EXECUTION + extra_time)
    return {"task_id": task_id, "result": "Task completed at " + str(datetime.now())}
