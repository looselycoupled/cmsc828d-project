import json

import numpy
import redis
from pymongo import MongoClient


def redis_db():
    return redis.Redis(host='localhost', port=6379, db=0)

def mongo_db():
    m = MongoClient()
    return m.airflow

def mongo_dagrun_doc(dagrun):
    return {
        "dag": dagrun.dag_id,
        "run_id": dagrun.run_id,
    }


def create_key(dag, dagrun, task_id):
    return "{}__{}__{}".format(dag.dag_id, dagrun.run_id, task_id)


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
