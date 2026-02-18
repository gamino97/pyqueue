import json
import time

import redis


def process_task(task):
    task_type = task["type"]
    payload = task["payload"]

    print(f"Procesando {task_type}...")
    # Simulación de tarea pesada
    time.sleep(2)
    print(f"Finalizado: {payload}")


class PyQueue:
    def __init__(self, name, broker: str):
        self.name = name
        self.broker = broker
        self.client = redis.Redis.from_url(broker)

    def start_worker(self):
        print("Worker escuchando tareas...")
        while True:
            # BRPOP espera hasta que haya algo en 'job_queue'
            # Retorna una tupla (nombre_cola, datos)
            _, data = self.client.brpop("job_queue")
            task = json.loads(data)
            process_task(task)

    def add_task(self, task):
        pass

    def add_job(self, task_type, payload):
        job = {"type": task_type, "payload": payload}
        self.client.lpush("job_queue", json.dumps(job))
        print(f"Tarea enviada: {task_type}")
