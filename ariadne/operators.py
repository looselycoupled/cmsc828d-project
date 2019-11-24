import json
import numpy

from airflow.operators.bash_operator import BashOperator as BaseBashOperator
from airflow.operators.python_operator import PythonOperator as BasePythonOperator
from airflow.operators.dummy_operator import DummyOperator as BaseDummyOperator
import redis

def create_key(dag, dagrun):
    return "{}__{}".format(dag.dag_id, dagrun.run_id)


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



class BashOperator(BaseBashOperator): pass


class PythonOperator(BasePythonOperator):

    def execute(self, *args, **kwargs):
        # load from redis
        r = redis.Redis(host='localhost', port=6379, db=0)

        key = create_key(kwargs["context"]["dag"], kwargs["context"]["dag_run"])
        incoming = r.get(key)
        if incoming is not None:
            incoming = json.loads(incoming)

        kwargs["context"]["data"] = incoming["data"]
        results = super().execute(*args, **kwargs)

        # save from redis
        payload = {
            "meta": {
                "context": {},
            },
            "data": results
        }

        r.set(key, json.dumps(payload, cls=AriadneEncoder))

class DummyOperator(BaseDummyOperator): pass

