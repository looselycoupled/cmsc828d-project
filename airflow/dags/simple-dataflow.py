# fix import paths
import os
import sys
sys.path.append(
    os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir
    ))
)

from airflow import DAG
from ariadne.operators import *
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd



default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 23),
    'email': ['allen.leis@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('simple-dataflow', default_args=default_args)

def csv_pop(data, *args, **kwargs):
    # print(args)
    # print(kwargs)
    # print(os.getcwd())

    print("data: {}".format(data))

    path = "data/csv/aapl.csv"
    df = pd.read_csv(path)

    if df.size > 0:
        item = df.iloc[0].to_dict()
        df.drop(0, inplace=True)
        df.to_csv(path, index=False)
        return item

t1 = PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=csv_pop,
    dag=dag,
)

def handler(data, *args, **kwargs):
    print(data)
    priceRange = data["High"] - data["Low"]
    print(f"priceRange: {priceRange}")

t2 = PythonOperator(
    task_id='handler',
    provide_context=True,
    python_callable=handler,
    dag=dag,
)

t1 >> t2
