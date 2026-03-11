#!/usr/bin/env python3
"""Task scheduler — work-stealing, priority queues, and rate limiting.

One file. Zero deps. Does one thing well.

Implements multiple scheduling strategies: FIFO, priority, round-robin,
work-stealing (Cilk-style), and token bucket rate limiting.
"""
import time, sys, random, threading
from collections import deque
import heapq

class Task:
    __slots__ = ('id', 'priority', 'fn', 'args', 'result', 'created', 'started', 'finished')
    _counter = 0
    def __init__(self, fn, args=(), priority=0):
        Task._counter += 1
        self.id = Task._counter
        self.priority = priority
        self.fn = fn
        self.args = args
        self.result = None
        self.created = time.time()
        self.started = None
        self.finished = None
    def __lt__(self, other):
        return self.priority < other.priority
    def run(self):
        self.started = time.time()
        self.result = self.fn(*self.args)
        self.finished = time.time()
        return self.result
    @property
    def latency(self):
        return (self.finished - self.created) * 1000 if self.finished else None

class FIFOScheduler:
    def __init__(self):
        self.queue = deque()
    def submit(self, task):
        self.queue.append(task)
    def run_all(self):
        results = []
        while self.queue:
            task = self.queue.popleft()
            task.run()
            results.append(task)
        return results

class PriorityScheduler:
    def __init__(self):
        self.heap = []
    def submit(self, task):
        heapq.heappush(self.heap, task)
    def run_all(self):
        results = []
        while self.heap:
            task = heapq.heappop(self.heap)
            task.run()
            results.append(task)
        return results

class RoundRobinScheduler:
    def __init__(self, num_queues=4):
        self.queues = [deque() for _ in range(num_queues)]
        self.next_queue = 0
    def submit(self, task):
        self.queues[self.next_queue].append(task)
        self.next_queue = (self.next_queue + 1) % len(self.queues)
    def run_all(self):
        results = []
        active = True
        while active:
            active = False
            for q in self.queues:
                if q:
                    task = q.popleft()
                    task.run()
                    results.append(task)
                    active = True
        return results

class WorkStealingScheduler:
    """Cilk-style work-stealing with per-worker deques."""
    def __init__(self, num_workers=4):
        self.num_workers = num_workers
        self.deques = [deque() for _ in range(num_workers)]
        self.next = 0

    def submit(self, task):
        self.deques[self.next].append(task)
        self.next = (self.next + 1) % self.num_workers

    def run_all(self):
        results = []
        for worker_id in range(self.num_workers):
            while True:
                # Try own deque (LIFO for locality)
                if self.deques[worker_id]:
                    task = self.deques[worker_id].pop()
                else:
                    # Steal from others (FIFO)
                    task = None
                    for i in range(self.num_workers):
                        if i != worker_id and self.deques[i]:
                            task = self.deques[i].popleft()
                            break
                if task is None:
                    break
                task.run()
                results.append(task)
        return results

class TokenBucket:
    """Rate limiter using token bucket algorithm."""
    def __init__(self, rate, burst):
        self.rate = rate      # tokens per second
        self.burst = burst    # max tokens
        self.tokens = burst
        self.last = time.time()

    def acquire(self, n=1):
        now = time.time()
        self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

def main():
    random.seed(42)
    work = lambda x: x * x

    print("=== Task Scheduler Comparison ===\n")
    for name, sched_cls in [("FIFO", FIFOScheduler), ("Priority", PriorityScheduler),
                             ("RoundRobin", lambda: RoundRobinScheduler(4)),
                             ("WorkStealing", lambda: WorkStealingScheduler(4))]:
        sched = sched_cls() if callable(sched_cls) else sched_cls()
        for i in range(20):
            sched.submit(Task(work, (i,), priority=random.randint(0, 10)))
        results = sched.run_all()
        order = [r.result for r in results[:5]]
        print(f"  {name:14s}: {len(results)} tasks, first 5 results: {order}")

    print("\n=== Token Bucket Rate Limiter ===")
    tb = TokenBucket(rate=10, burst=5)
    accepted = rejected = 0
    for _ in range(20):
        if tb.acquire():
            accepted += 1
        else:
            rejected += 1
    print(f"  20 requests, rate=10/s, burst=5: accepted={accepted}, rejected={rejected}")

if __name__ == "__main__":
    main()
