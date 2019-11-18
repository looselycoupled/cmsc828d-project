from airflow.operators.bash_operator import BashOperator as BaseBashOperator
from airflow.operators.python_operator import PythonOperator as BasePythonOperator
from airflow.operators.dummy_operator import DummyOperator as BaseDummyOperator



class BashOperator(BaseBashOperator): pass



class PythonOperator(BasePythonOperator):

    def execute(self, *args, **kwargs):
        print("starting")
        super().execute(*args, **kwargs)
        print("ending")


class DummyOperator(BaseDummyOperator): pass

