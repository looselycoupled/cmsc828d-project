import json
import time

import numpy
import redis
from pymongo import MongoClient


##########################################################################
## Database Related
##########################################################################

def redis_db():
    return redis.Redis(host='localhost', port=6379, db=0)

def mongo_db():
    m = MongoClient()
    return m.ariadne

def mongo_dagrun_doc(dagrun):
    return {
        "dag": dagrun.dag_id,
        "run_id": dagrun.run_id,
    }

def create_key(dag, dagrun, task_id):
    return "{}__{}__{}".format(dag.dag_id, dagrun.run_id, task_id)


##########################################################################
## JSON Encoders
##########################################################################

class AriadneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(AriadneEncoder, self).default(obj)




##########################################################################
## Timer Class
##########################################################################

"""
Timing utility functions and helpers
"""

def human_readable_time(s):
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return "{:>02.0f}:{:02.0f}:{:>07.4f}".format(h, m, s)


class Timer:
    """
    A context object timer. Usage:
        >>> with Timer() as timer:
        ...     do_something()
        >>> print timer.interval
    """
    def __init__(self):
        self.time = time.time

    def __enter__(self):
        self.start = self.time()
        return self

    def __exit__(self, *exc):
        self.finish = self.time()
        self.interval = self.finish - self.start

    def __str__(self):
        return human_readable_time(self.interval)