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

dag = DAG('test-branch', default_args=default_args)


def waste_time(incr=100000000):
    iters = int(random.random() * incr)
    for i in range(iters):
        v = i * i / 2


def coin_flip(*args, **kwargs):
    if kwargs.get("test_mode", False):
        web_pdb.set_trace()

    waste_time()
    return random.randint(0,1)


def choose(data, *args, choices, **kwargs):
    if kwargs.get("test_mode", False):
        web_pdb.set_trace()

    if data:
        return choices[1]
    return choices[0]


t1 = PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

b1 = BranchPythonOperator(
    task_id='b1',
    provide_context=True,
    python_callable=choose,
    op_kwargs={"choices": ["t2", "t3"]},
    dag=dag,
)


t2 = PythonOperator(
    task_id='t2',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

b2 = BranchPythonOperator(
    task_id='b2',
    provide_context=True,
    python_callable=choose,
    op_kwargs={"choices": ["t4", "t5"]},
    dag=dag,
)

t4 = PythonOperator(
    task_id='t4',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

t5 = PythonOperator(
    task_id='t5',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

t3 = PythonOperator(
    task_id='t3',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

b3 = BranchPythonOperator(
    task_id='b3',
    provide_context=True,
    python_callable=choose,
    op_kwargs={"choices": ["t6", "t7"]},
    dag=dag,
)


t6 = PythonOperator(
    task_id='t6',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)

t7 = PythonOperator(
    task_id='t7',
    provide_context=True,
    python_callable=coin_flip,
    dag=dag,
)


t1 >> b1 >> [t2, t3]

t2 >> b2 >> [t4, t5]

t3 >> b3 >> [t6, t7]
