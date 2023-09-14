# Redis Queue with FastAPI POC
Mock as a POC using redis queues to get estimated time left for tasks 

## Installation
To install the project, use your favorite environment manager.

Install all needed packages, for example:
```sh
pip install -r requirements.txt
```
Redis is also needed, you can install it with docker:
```sh
docker run -p 6379:6379 -d redis:5
```


## Usage
To run the project, you first need to start the redis server. 
To make it easier you can simply do it from the docker-compose file:
```sh
docker compose up
```
Then you can run the project with uvicorn
```sh
uvicorn src.app:app 
```


