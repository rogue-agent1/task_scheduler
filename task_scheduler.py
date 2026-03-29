#!/usr/bin/env python3
"""Task scheduler: cron parsing, DAG execution, retries, priorities."""
import sys, time, re, heapq
from collections import defaultdict

class CronExpr:
    def __init__(self, expr):
        parts = expr.split(); assert len(parts)==5
        self.minute,self.hour,self.dom,self.month,self.dow = [self._parse(p) for p in parts]
    def _parse(self, field):
        if field == '*': return None
        if '/' in field: base, step = field.split('/'); return ('step', int(step))
        if ',' in field: return ('list', [int(x) for x in field.split(',')])
        if '-' in field: lo, hi = field.split('-'); return ('range', int(lo), int(hi))
        return ('exact', int(field))
    def describe(self):
        def desc(f, name):
            if f is None: return f"every {name}"
            if f[0] == 'exact': return f"at {name} {f[1]}"
            if f[0] == 'step': return f"every {f[1]} {name}s"
            if f[0] == 'list': return f"at {name}s {f[1]}"
            if f[0] == 'range': return f"{name} {f[1]}-{f[2]}"
        return ", ".join(filter(None, [desc(self.minute,"min"),desc(self.hour,"hour")]))

class Task:
    def __init__(self, name, fn, deps=None, priority=0, retries=3):
        self.name,self.fn,self.deps = name,fn,deps or []
        self.priority,self.retries,self.attempts = priority,retries,0
        self.state = "pending"; self.result = None
    def __lt__(self, other): return self.priority > other.priority

class DAGScheduler:
    def __init__(self): self.tasks = {}; self.completed = set()
    def add(self, task): self.tasks[task.name] = task
    def _ready(self):
        return [t for t in self.tasks.values() if t.state=="pending" and all(d in self.completed for d in t.deps)]
    def run(self):
        log = []
        while True:
            ready = self._ready()
            if not ready: break
            ready.sort()
            for task in ready:
                task.attempts += 1
                try:
                    task.result = task.fn(); task.state = "done"
                    self.completed.add(task.name)
                    log.append(f"OK: {task.name} -> {task.result}")
                except Exception as e:
                    if task.attempts >= task.retries:
                        task.state = "failed"; log.append(f"FAIL: {task.name} ({e})")
                    else: log.append(f"RETRY: {task.name} ({task.attempts}/{task.retries})")
        return log

def main():
    cron = CronExpr("*/5 9-17 * * 1-5")
    print(f"  Cron: {cron.describe()}")
    dag = DAGScheduler()
    dag.add(Task("fetch", lambda: "data_ok"))
    dag.add(Task("parse", lambda: "parsed", deps=["fetch"]))
    dag.add(Task("validate", lambda: "valid", deps=["parse"]))
    dag.add(Task("transform", lambda: "transformed", deps=["validate"]))
    dag.add(Task("load", lambda: "loaded", deps=["transform"]))
    log = dag.run()
    for entry in log: print(f"  {entry}")

if __name__ == "__main__": main()
