import time
from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import State
from airflow.operators.dagrun_operator import DagRunOrder, TriggerDagRunOperator

from ariadne.utils import redis_db, mongo_db, create_key, AriadneEncoder, mongo_dagrun_doc, Timer

class EnqueueDagRunsOperator(TriggerDagRunOperator):

    def execute(self, context):

        session = settings.Session()

        for inflow in self.python_callable():
            d = DagRunOrder(payload=inflow)
            dt = datetime.datetime.utcnow()
            d.run_id = 'trig__' + dt.isoformat()

            db = DagBag(settings.DAGS_FOLDER)
            # trigger_dag = db.get_dag(self.trigger_dag_id)

            dr = db.get_dag(self.trigger_dag_id).create_dagrun(
                run_id=d.run_id,
                execution_date=dt,
                state=State.RUNNING,
                conf=d.payload,
                external_trigger=True,
            )
            created_dr_ids.append(dr.id)
            self.log.info("Enqueued: %s, %s", dr, dt)
            time.sleep(1)

        session.commit()
        session.close()