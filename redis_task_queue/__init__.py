#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import json
import time

class RedisQueue():
    """Manage the Redis queues with simple atomic functions."""

    def __init__(self, host='localhost', port=6379, db=0, logger=None, task_name="id", request_queues=['default'], prefix=''):
        self.client = redis.StrictRedis(host=host, port=port, db=db)
        # List queues in order of priority (highest priority first)
        # Is it a queue of queues? Queueception!
        self.task_name = task_name
        self.logger = logger
        self.request_queues = request_queues
        self.prefix = prefix

    def get_task(self):
        """Pull a tasks from one of the queues, in order of their priority."""

        for queue in self.request_queues:
            request = self.client.lpop(queue)
            if request:
                task_id = str(request)

                task = json.loads(self.client.get(self.prefix + task_id))

                if task["status"] == "pending":
                    task["status"] = "processing"
                    task[self.task_name] = task_id
                    task["started_on"] = time.strftime("%H:%M:%S")
                    self.client.set(self.prefix + task_id, json.dumps(task))
                    if self.logger:
                        self.logger.info("--> Request found for %s %s: %s",
                                    self.task_name, "(" + queue + ")",
                                    task_id)
                    return task
        return None

    def complete(self, task, data):
        """Set the task as complete, and set its corresponding data."""

        task["data"] = data
        task["completed_on"] = time.strftime("%H:%M:%S")
        task["status"] = "completed"
        self.client.set(self.prefix + task[self.task_name], json.dumps(task))
        if self.logger:
            self.logger.info("--> Task done for %s: %s", self.task_name, task[self.task_name])

    def in_progress(self, task, data):
        """Set the task as complete, and set its corresponding data."""

        task["data"] = data
        task["completed_on"] = time.strftime("%H:%M:%S")
        task["status"] = "processing"
        self.client.set(self.prefix + task[self.task_name], json.dumps(task))
        if self.logger:
            self.logger.info("--> Task in progress for %s: %s", self.task_name, task[self.task_name])

    def abort(self, task, data=None):
        """Set the task as aborted."""

        task["data"] = None
        task["completed_on"] = None
        task["status"] = "aborted"
        self.client.set(self.prefix + task[self.task_name], json.dumps(task))
        if self.logger:
            self.logger.info("--> Task *aborted* for %s: %s",
                        self.task_name, task[self.task_name])
