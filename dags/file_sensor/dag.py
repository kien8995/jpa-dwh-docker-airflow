from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from dags.file_sensor.sensor import TextFileSensorTask

default_args = {
    "owner": "kien tran",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 22),
    'provide_context': True,
    'max_active_runs': 1,
    "email": ["kientd.aits@vietnamairlines.com"],
    "email_on_failure": ["kientd.aits@vietnamairlines.com"],
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
}

dag = DAG("file_sensor", default_args=default_args, schedule_interval=None)

with dag:
    start = DummyOperator(
        task_id='start'
    )

    end = TriggerDagRunOperator(
        task_id='end',
        trigger_dag_id="file_sensor",
    )

    textFileTask = TextFileSensorTask(dag, '/usr/local/airflow/dags/', '([a-zA-Z0-9])+(.txt)$').tasks()
    start >> textFileTask[0]
    textFileTask[1] >> end