import asyncio
from typing import List, Dict, Set
import random
import time

class SwarmManager:
    def __init__(self):
        self.active_workers: Set[str] = set()
        self.worker_loads: Dict[str, int] = {}
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.results: Dict[str, any] = {}
        self.worker_heartbeats: Dict[str, float] = {}
        self.HEARTBEAT_TIMEOUT = 30  # seconds

    async def register_worker(self, worker_id: str) -> None:
        self.active_workers.add(worker_id)
        self.worker_loads[worker_id] = 0
        self.worker_heartbeats[worker_id] = time.time()
        print(f'Worker {worker_id} registered')

    async def deregister_worker(self, worker_id: str) -> None:
        if worker_id in self.active_workers:
            self.active_workers.remove(worker_id)
            del self.worker_loads[worker_id]
            del self.worker_heartbeats[worker_id]
            print(f'Worker {worker_id} deregistered')

    async def heartbeat(self, worker_id: str) -> None:
        if worker_id in self.active_workers:
            self.worker_heartbeats[worker_id] = time.time()

    async def check_worker_health(self) -> None:
        while True:
            current_time = time.time()
            dead_workers = [
                worker_id for worker_id, last_beat in self.worker_heartbeats.items()
                if current_time - last_beat > self.HEARTBEAT_TIMEOUT
            ]
            
            for worker_id in dead_workers:
                print(f'Worker {worker_id} appears dead, redistributing tasks')
                await self.handle_worker_failure(worker_id)
            
            await asyncio.sleep(5)

    async def handle_worker_failure(self, worker_id: str) -> None:
        tasks_to_reassign = []
        for task_id, assigned_worker in self.task_assignments.items():
            if assigned_worker == worker_id:
                tasks_to_reassign.append(task_id)
        
        await self.deregister_worker(worker_id)
        
        for task_id in tasks_to_reassign:
            await self.task_queue.put(self.tasks[task_id])

    async def assign_task(self, task: Dict) -> str:
        if not self.active_workers:
            raise RuntimeError('No active workers available')

        # Find worker with minimum load
        worker_id = min(self.worker_loads.items(), key=lambda x: x[1])[0]
        self.worker_loads[worker_id] += 1
        return worker_id

    async def complete_task(self, worker_id: str, task_id: str, result: any) -> None:
        if worker_id in self.active_workers:
            self.worker_loads[worker_id] -= 1
            self.results[task_id] = result

    async def get_worker_status(self) -> Dict:
        return {
            'active_workers': len(self.active_workers),
            'total_tasks_pending': self.task_queue.qsize(),
            'worker_loads': self.worker_loads.copy()
        }

    def get_results(self) -> Dict:
        return self.results.copy()

    async def start(self) -> None:
        health_check = asyncio.create_task(self.check_worker_health())
        await health_check