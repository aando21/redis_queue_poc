import asyncio
import json

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from src_async.worker import Worker
import src_async.config as config
from uuid import uuid4
from src_async.utils import redis_db, redis_task_queue_push, redis_result_hash_pop


# Instantiate the API and the redis database
app = FastAPI()
db = redis_db()

# get worker
worker = Worker(db)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(worker.run_main())


@app.post("/add_task")
async def add_task() -> JSONResponse:
    """Add a task to the Redis queue."""
    job_id = str(uuid4())
    message = {
        "id": job_id,
        "task": "perform_task",
    }
    await redis_task_queue_push(db, json.dumps(message))
    return JSONResponse({"message": "Task added to the queue", "job_id": job_id})


@app.get("/check-result/{job_id}")
async def check_result(job_id: str) -> JSONResponse:
    """
    Checks the result of a task. There are four scenarios:
    1. The task is waiting on the queue to be executed.
    2. The task is running.
    3. The task is finished and the result is available.
    4. The id is not found in the queue or in the result hash. This means that the task was not found.
    """
    # Fetch the job result from the queue
    hash_results = await db.hgetall(config.REDIS_RESULT_HASH_NAME)
    hash_results = {key.decode(): value.decode() for key, value in hash_results.items()}
    if job_id in hash_results and hash_results[job_id] == "working in progress":
        return JSONResponse({"status": "Job is running. Try again in a few seconds."})
    elif job_id in hash_results:
        result = await redis_result_hash_pop(db, job_id)
        result = json.loads(result.decode())
        return JSONResponse({"status": "Job finished", "result": result["result"]})

    pending_tasks = await db.lrange(config.REDIS_TASK_QUEUE_NAME, 0, -1)
    for i, task in enumerate(pending_tasks):
        task_id = json.loads(task.decode())["id"]
        if task_id == job_id:
            position = i
            tasks_ahead = len(pending_tasks) - position - 1
            return JSONResponse(
                {
                    "status": "Job is pending",
                    "tasks_ahead": tasks_ahead,
                    "estimated_time": tasks_ahead * config.TIME_OF_TASK_EXECUTION,
                }
            )

    return JSONResponse(
        {
            "status": "Job not found. It may be running or failed. Try again in 30 seconds."
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
