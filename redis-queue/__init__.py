#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import json
import time
# from config import LOGGER

class ModelQueue():
    """Manage the Redis queues with simple atomic functions."""

    def __init__(self, host, port, db, LOGGER):
        self.client = redis.StrictRedis(host=host, port=port, db=db)
        # List queues in order of priority (highest priority first)
        # Is it a queue of queues? Queueception!
        self.request_queues = ['model_computation_user', 'model_computation']

    def get_task(self):
        """Pull a tasks from one of the queues, in order of their priority."""

        for queue in self.request_queues:
            request = self.client.lpop(queue)
            if request:
                census_tract = str(request)
                assert len(census_tract) == 11
                is_user = (queue == 'model_computation_user')
                task = json.loads(self.client.get('model_' + census_tract))

                if task["status"] == "pending":
                    task["status"] = "processing"
                    task["census_tract"] = census_tract
                    task["started_on"] = time.strftime("%H:%M:%S")
                    self.client.set('model_' + census_tract, json.dumps(task))
                    if LOGGER:
                        LOGGER.info("--> Request found for census_tract%s: %s",
                                    "(user)" if is_user else "",
                                    census_tract)
                    return task
        return None

    def complete(self, task, model):
        """Set the task as complete, and set its corresponding model."""

        task["coefficients"] = model
        task["completed_on"] = time.strftime("%H:%M:%S")
        task["status"] = "completed"
        self.client.set('model_' + task["census_tract"], json.dumps(task))
        if LOGGER:
            LOGGER.info("--> Task done for census_tract: %s - queue size: %d",
                        task["census_tract"],
                        self.client.llen('model_computation_user'))

    def abort(self, task):
        """Set the task as aborted."""

        task["coefficients"] = None
        task["completed_on"] = None
        task["status"] = "aborted"
        self.client.set('model_' + task["census_tract"], json.dumps(task))
        if LOGGER:
            LOGGER.info("--> Task *aborted* for census_tract: %s - queue size: %d",
                        task["census_tract"],
                        self.client.llen('model_computation_user'))
