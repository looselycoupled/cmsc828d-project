import os
import sys
import json
import time
import threading
import traceback

import web_pdb
import psutil
import numpy
from pprint import pprint
import time

from airflow.models import SkipMixin

from airflow.operators.bash_operator import BashOperator as BaseBashOperator
from airflow.operators.python_operator import PythonOperator as BasePythonOperator
from airflow.operators.dummy_operator import DummyOperator as BaseDummyOperator

from ariadne.utils import redis_db, mongo_db, create_key, AriadneEncoder, mongo_dagrun_doc, Timer


# debug = lambda: web_pdb.set_trace()

class DummyOperator(BaseDummyOperator):
    pass


class BashOperator(BaseBashOperator):
    pass


class PythonOperator(BasePythonOperator):

    def upsert_dagrun_doc(self, context):
        collection = mongo_db().task_data
        query = mongo_dagrun_doc(context["dag_run"])
        doc = query.copy()
        doc["task_executions"] = []
        collection.find_one_and_update(
            query,
            {"$setOnInsert": doc},
            upsert=True
        )

    def export(self, context, inflow, outflow, elapsed, task_times, error):
        collection = mongo_db().task_data
        query = mongo_dagrun_doc(context["dag_run"])

        # construct task instance record
        task_instance_details = {
            "task_id": context["ti"].task_id,
            "inflow": inflow,
            "outflow": outflow,
            "parents": [t.task_id for t in context["ti"].task.upstream_list],
            "context": {
                "dag_id": context["dag"].dag_id,
                "dagrun_id": context["dag_run"].run_id,
                "task_id": context["ti"].task_id,
                "ts": context["ts"],
            },
            "resource_utilization": task_times,
            "elapsed": elapsed,
            "error": error,
        }

        # convert numpy fields using json encoder
        task_instance_details = json.loads(json.dumps(task_instance_details, cls=AriadneEncoder))

        # upsert into mongodb
        collection.find_one_and_update(
            query,
            {"$push": {"task_executions": task_instance_details}}
        )


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


    def pre_execute(self, context):
        # ensure mongodb doc exists for this dagrun
        self.upsert_dagrun_doc(context)


    def watcher(self, times):
        t = threading.currentThread()
        p = psutil.Process()
        while getattr(t, "go", True):

            with p.oneshot():
                times.append({
                    "time": time.time(),
                    "cpu_times": dict(p.cpu_times()._asdict()),
                    "virtual_memory": dict(psutil.virtual_memory()._asdict()),
                    "swap_memory": dict(psutil.swap_memory()._asdict()),
                    "memory_info": dict(p.memory_info()._asdict()),
                    "memory_usage": p.memory_info().rss
                })
            time.sleep(.04)


    def execute(self, *args, **kwargs):

        # collect incoming data and add to context
        incoming = self.inflow(kwargs["context"])
        kwargs["context"]["data"] = incoming
        task_times = []
        error = False

        thrd = threading.Thread(target=self.watcher, args=(task_times,))
        thrd.start()

        # execute user callable
        output = None
        errinfo = {
            "errored": False,
        }
        with Timer() as t:
            try:
                output = super().execute(*args, **kwargs)
            except Exception as e:
                exc_type, exc_value, exc_tb = sys.exc_info()
                tb = traceback.TracebackException(*sys.exc_info())
                fmt_tb = "".join(list(tb.format()))

                filename, linenum, funcname, source = traceback.extract_tb(exc_tb)[-1]
                with open(filename, "r") as f:
                    sourcecode = f.readlines()
                # fline = max(linenum - 1, 1) - 1
                # eline = min(linenum + 1, len(sourcecode)) - 1
                # questionable_code = sourcecode[fline:eline]

                # template = '{filename:<23}:{linenum}:{funcname}:\n    {source}'
                # condensed_tb = template.format(
                #     filename=os.path.basename(filename),
                #     linenum=linenum,
                #     source=source,
                #     funcname=funcname
                # )

                errinfo = {
                    "errored": True,
                    "tb": fmt_tb,
                    # "condensed_tb": condensed_tb,
                    "exc": e.__repr__(),
                    "msg": str(e),
                    "sourcecode": sourcecode,
                    "linenum": linenum,
                    "funcname": funcname,
                    "source": source,
                    "filename": filename,
                }
                raise
            finally:
                # save to mongodb
                # import pdb; pdb.set_trace()
                self.export(
                    kwargs["context"],
                    incoming,
                    output,
                    t.interval,
                    task_times,
                    errinfo
                )


                thrd.go = False
                thrd.join()

        # save outgoing to redis
        self.outflow(kwargs["context"], output)


        return output


class BranchPythonOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to "branch" or follow a path following the execution
    of this task.
    It derives the PythonOperator and expects a Python function that returns
    a single task_id or list of task_ids to follow. The task_id(s) returned
    should point to a task directly downstream from {self}. All other "branches"
    or directly downstream tasks are marked with a state of ``skipped`` so that
    these paths can't move forward. The ``skipped`` states are propagated
    downstream to allow for the DAG state to fill up and the DAG run's state
    to be inferred.
    """

    def execute(self, context):
        branch = super().execute(context=context)
        self.skip_all_except(context['ti'], branch)
