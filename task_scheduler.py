#!/usr/bin/env python3
"""Task scheduler with dependencies, priorities, and topological sorting."""
import sys, heapq
from collections import defaultdict

class Task:
    def __init__(self, name, priority=0, duration=1, deps=None):
        self.name = name; self.priority = priority; self.duration = duration
        self.deps = set(deps or []); self.status = "pending"; self.start = None; self.end = None

class Scheduler:
    def __init__(self): self.tasks = {}; self.graph = defaultdict(set); self.rdeps = defaultdict(set)

    def add(self, name, priority=0, duration=1, deps=None):
        t = Task(name, priority, duration, deps)
        self.tasks[name] = t
        for d in t.deps: self.graph[d].add(name); self.rdeps[name].add(d)
        return t

    def topo_sort(self):
        in_deg = {n: len(self.rdeps[n]) for n in self.tasks}
        queue = [(-self.tasks[n].priority, n) for n in self.tasks if in_deg[n] == 0]
        heapq.heapify(queue); order = []
        while queue:
            _, name = heapq.heappop(queue); order.append(name)
            for dep in self.graph[name]:
                in_deg[dep] -= 1
                if in_deg[dep] == 0: heapq.heappush(queue, (-self.tasks[dep].priority, dep))
        if len(order) != len(self.tasks): raise ValueError("Cycle detected!")
        return order

    def schedule(self, workers=1):
        order = self.topo_sort(); time = 0; timeline = []
        ready = []; done = set(); running = []
        remaining = {n: set(self.rdeps[n]) for n in self.tasks}
        idx = 0
        while done != set(self.tasks.keys()):
            # Add newly ready tasks
            for n in list(self.tasks.keys()):
                if n not in done and n not in [r[1] for r in running] and not remaining[n] and n not in [r[1] for r in ready]:
                    heapq.heappush(ready, (-self.tasks[n].priority, n))
            # Start tasks on free workers
            while ready and len(running) < workers:
                _, name = heapq.heappop(ready)
                t = self.tasks[name]; t.start = time; t.status = "running"
                running.append((time + t.duration, name))
                timeline.append((name, time, time + t.duration))
            if not running: break
            running.sort()
            finish_time, name = running.pop(0)
            time = finish_time; done.add(name); self.tasks[name].status = "done"; self.tasks[name].end = time
            for dep in self.graph[name]: remaining[dep].discard(name)
        return timeline, time

    def gantt(self, timeline, total):
        W = 50; print(f"\nGantt Chart (total: {total} units)\n")
        for name, start, end in timeline:
            bar_s = int(start/total*W); bar_e = int(end/total*W)
            bar = " "*bar_s + "█"*(bar_e-bar_s)
            print(f"  {name:12s} |{bar:<{W}}| {start}-{end}")

def demo():
    s = Scheduler()
    s.add("compile", priority=3, duration=3)
    s.add("lint", priority=2, duration=1)
    s.add("test", priority=3, duration=4, deps=["compile"])
    s.add("docs", priority=1, duration=2, deps=["compile"])
    s.add("package", priority=4, duration=2, deps=["test", "docs", "lint"])
    s.add("deploy", priority=5, duration=1, deps=["package"])
    print("=== Task Scheduler ===")
    print(f"Topological order: {s.topo_sort()}")
    tl, total = s.schedule(workers=2)
    s.gantt(tl, total)

def main(): demo()
if __name__ == "__main__": main()
