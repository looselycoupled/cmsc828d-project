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
import arrow
import requests


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 17),
    'email': ['allen.leis@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('usgs-download', default_args=default_args, schedule_interval=timedelta(hours=1))



def download(*args, **kwargs):
    # pprint(kwargs)
    t = arrow.get(kwargs["ts"])
    start = t.datetime.replace(microsecond=0,second=0,minute=0) - timedelta(hours=1)
    end = start + timedelta(minutes=59, seconds=59)


    url_template = "https://geomag.usgs.gov/ws/edge/?id=BOU&starttime={}&endtime={}&elements=H,D,Z,F"
    url = url_template.format(start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("url: {}", url)

    response = requests.get(url)
    print("response: {}", response.status_code)

    with open("tmp.iaga", "w") as f:
        f.write(response.text)

    return 'download complete...'


t1_download = PythonOperator(
    task_id='download',
    provide_context=True,
    python_callable=download,
    dag=dag,
)

t2_dummy = DummyOperator(
    task_id='dummy_blue',
    dag=dag,
)
t3_dummy = DummyOperator(
    task_id='dummy_red',
    dag=dag,
)
t4_dummy = DummyOperator(
    task_id='dummy_green',
    dag=dag,
)

t1_download >> t2_dummy

t2_dummy >> t3_dummy
t2_dummy >> t4_dummy