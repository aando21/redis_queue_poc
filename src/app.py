import json
import threading

from fastapi import FastAPI
from src.worker import Worker
import src.config as config
from uuid import uuid4
from src.utils import redis_db, redis_task_queue_push, redis_result_hash_pop


# Instantiate the API and the redis database
app = FastAPI()
db = redis_db()

# get worker
worker = Worker(db)


@app.on_event("startup")
async def startup_event():
    background = threading.Thread(target=worker.start_worker)
    background.start()


@app.post("/add_task")
async def add_task():
    # Add a task to the Redis queue
    job_id = str(uuid4())
    message = {
        "id": job_id,
        "task": "src.redis_task.perform_task",
    }
    redis_task_queue_push(db, json.dumps(message))
    return {"message": "Task added to the queue", "job_id": job_id}


@app.get("/check-result/{job_id}")
async def check_result(job_id: str):
    """
    Checks the result of a task. There are five scenarios:
    1. The job failed. We should return that to the user.
    2. The task is waiting on the queue to be executed.
    3. The task is running.
    4. The task is finished and the result is available.
    5. The id is not found in the queue or in the result hash. This means that the task was not found.
    """
    # Fetch the job result from the queue
    hash_results = db.hgetall(config.REDIS_RESULT_HASH_NAME)
    hash_results = {key.decode(): value.decode() for key, value in hash_results.items()}
    # Check if the job failed
    if job_id in hash_results and hash_results[job_id].startswith("Encountered an error"):
        return {"status": "Job failed", "error": hash_results[job_id]}
    # Check if the job is running
    if job_id in hash_results and hash_results[job_id] == "working in progress":
        return {"status": "Job is running. Try again in a few seconds."}
    # Check if the job is finished
    elif job_id in hash_results:
        result = await redis_result_hash_pop(db, job_id)
        result = json.loads(result.decode())
        return {"status": "Job finished", "result": result["result"]}

    # Check if the job is pending
    pending_tasks = db.lrange(config.REDIS_TASK_QUEUE_NAME, 0, -1)
    for i, task in enumerate(pending_tasks):
        task_id = json.loads(task.decode())["id"]
        if task_id == job_id:
            position = i
            tasks_ahead = len(pending_tasks) - position - 1
            return {
                "status": "Job is pending",
                "tasks_ahead": tasks_ahead,
                "estimated_time": tasks_ahead * config.TIME_OF_TASK_EXECUTION,
            }
    # If the job is not found, it's not in the queue or the results.
    # It may be due to the fact that it was requested before.
    return {
        "status": "Job not found. "
                  "Please make sure that the job id is correct or that you haven't requested the job result before."
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
