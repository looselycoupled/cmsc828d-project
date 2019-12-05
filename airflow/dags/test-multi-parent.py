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

import random
from airflow import DAG
from ariadne.operators import *
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
import web_pdb

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

dag = DAG('test-multi-parent', default_args=default_args)

def csv_pop(data, *args, **kwargs):
    if kwargs.get("test_mode", False):
        web_pdb.set_trace()

    waste_time(10000000)
    path = "data/csv/aapl.csv"
    df = pd.read_csv(path)

    if df.size > 0:
        item = df.iloc[0].to_dict()
        df.drop(0, inplace=True)
        df.to_csv(path, index=False)

        item.pop("Volume")
        return item

t1 = PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=csv_pop,
    dag=dag,
)

def waste_time(incr=100000000):
    iters = int(random.random() * incr)
    for i in range(iters):
        v = i * i / 2

def handler(data, field, *args, **kwargs):
    if kwargs.get("test_mode", False):
        web_pdb.set_trace()
    waste_time()
    return data[field]

# def debug(**kwargs):
#     if kwargs.get("test_mode", False):
#         web_pdb.set_trace()


def handlerDiff(data, *args, **kwargs):
    if kwargs.get("test_mode", False):
        web_pdb.set_trace()

    print(data)
    waste_time()
    return data[1] - data[0]

t2 = PythonOperator(
    task_id='handlerOpen',
    provide_context=True,
    python_callable=handler,
    op_kwargs={"field": "Open"},
    dag=dag,
)
t3 = PythonOperator(
    task_id='handlerClose',
    provide_context=True,
    python_callable=handler,
    op_kwargs={"field": "Close"},
    dag=dag,
)

t4 = PythonOperator(
    task_id='handlerDiff',
    provide_context=True,
    python_callable=handlerDiff,
    dag=dag,
)

t1 >> [t2, t3] >> t4
