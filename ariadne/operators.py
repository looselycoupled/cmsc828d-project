import json
import numpy
from pprint import pprint

from airflow.operators.bash_operator import BashOperator as BaseBashOperator
from airflow.operators.python_operator import PythonOperator as BasePythonOperator
from airflow.operators.dummy_operator import DummyOperator as BaseDummyOperator

from ariadne.utils import redis_db, mongo_db, create_key, AriadneEncoder


class DummyOperator(BaseDummyOperator):
    pass


class BashOperator(BaseBashOperator):
    pass


class PythonOperator(BasePythonOperator):

    def save_task_metadata(self, context, results):
        collection = mongo_db().task_data

        # query = mongo_dagrun_doc(dagrun)

        payload = {
            "results": results,
            "context": {
                "dag_id": context["dag"].dag_id,
                "dagrun_id": context["dag_run"].run_id,
                "task_id": context["ti"].task_id,
                "ts": context["ts"],
                "parents": [t.task_id for t in context["ti"].task.upstream_list],
            }
        }

        # convert numpy fields using json encoder
        payload = json.loads(json.dumps(payload, cls=AriadneEncoder))

        # insert into mongodb
        collection.insert(payload)


    def inflow(self, context):
        print(context["ti"].task.upstream_list)
        r = redis_db()
        payload = []

        for upstream in [t.task_id for t in context["ti"].task.upstream_list]:
            val = r.get(create_key(context["dag"], context["dag_run"], upstream))
            if val is not None:
                val = json.loads(val)
                payload.append(val["data"])
            else:
                payload.append(None)

        if len(payload) == 1: return payload[0]

        # NOTE: items are sorted alphabetically by task_id
        return payload


    def outflow(self, context, results):
        """
        store results of task in redis for input to the next tasks
        """
        r = redis_db()
        key = create_key(context["dag"], context["dag_run"], context["ti"].task_id)
        payload = {
            "meta": {},
            "data": results
        }
        r.set(key, json.dumps(payload, cls=AriadneEncoder))


    def execute(self, *args, **kwargs):

        # collect incoming data and add to context
        incoming = self.inflow(kwargs["context"])
        kwargs["context"]["data"] = incoming

        # execute user callable
        results = super().execute(*args, **kwargs)

        # save outgoing to redis
        self.outflow(kwargs["context"], results)

        # save to mongodb
        self.save_task_metadata(kwargs["context"], results)



