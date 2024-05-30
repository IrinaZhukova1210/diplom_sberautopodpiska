import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/airflow_sber')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.ga_sessions import add_ga_sessions
from modules.ga_hits import add_ga_hits

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 5, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
        dag_id='sberautopodpiska_db',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    add_ga_sessions = PythonOperator(
        task_id='add_ga_sessions',
        python_callable=add_ga_sessions,
    )
    add_ga_hits = PythonOperator(
        task_id='add_ga_hits',
        python_callable=add_ga_hits,
    )

    add_ga_sessions >> add_ga_hits
