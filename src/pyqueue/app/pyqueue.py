import importlib
import json
import time
import uuid
from typing import TypedDict

import redis


def process_task(task):
    task_type = task["type"]
    payload = task["payload"]

    print(f"Procesando {task_type}...")
    # Simulación de tarea pesada
    time.sleep(2)
    print(f"Finalizado: {payload}")


class TaskDict(TypedDict):
    id: str
    name: str
    args: tuple
    kwargs: dict


class PyQueue:
    def __init__(self, name, broker: str):
        self.name = name
        self.broker = broker
        self.client: redis.Redis = redis.Redis.from_url(broker)
        self.tasks = {}

    def start_worker(self):
        print("Worker listening...")
        app = importlib.import_module(self.name)
        while True:
            # BRPOP espera hasta que haya algo en 'job_queue'
            # Retorna una tupla (nombre_cola, datos)
            _, data = self.client.brpop(["job_queue"])
            task: TaskDict = json.loads(data)
            task_func = getattr(app, task.get("name"))
            if task_func is None:
                continue
            task_func(*task.get("args"), **task.get("kwargs"))

    def add_task(self, task):
        self.tasks[task.__name__] = task
        task_class = Task(task, self.client)

        def execute_task(*args, **kwargs):
            task_dict: TaskDict = {
                "id": str(uuid.uuid4()),
                "name": task.__name__,
                "args": args,
                "kwargs": kwargs,
            }
            self.client.lpush("job_queue", json.dumps(task_dict))

        return task_class

    def add_job(self, task_type, payload):
        job = {"type": task_type, "payload": payload}
        self.client.lpush("job_queue", json.dumps(job))
        print(f"Tarea enviada: {task_type}")


class Task:

    def __init__(self, task, client: redis.Redis):
        self.task = task
        self.client = client

    def delay(self, *args, **kwargs):
        task_dict: TaskDict = {
            "id": str(uuid.uuid4()),
            "name": self.task.__name__,
            "args": args,
            "kwargs": kwargs,
        }
        self.client.lpush("job_queue", json.dumps(task_dict))
        return

    def __call__(self, *args, **kwargs):
        return self.task(*args, **kwargs)
