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
from ariadne.dag_operators import EnqueueDagRunsOperator
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

dag = DAG('test-enqueue-multi-parent', default_args=default_args)

# TODO: work in progress to trigger multiple dag runs from a dag...