from __future__ import print_function
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from request_data import GetData

default_args = {
    'start_date': datetime.datetime(2021,8,1),
    'schedule_interval': datetime.timedelta(days=1)
}

with DAG(
    'upload_finance_data',
    default_args=default_args) as dag:
    def upload_data():
        data_getter = GetData()
        data = data_getter.get_data()
        data_getter.set_cloud_cred()
        data_getter.upload_data(data)

    upload_operator = PythonOperator(
        task_id='upload_to_bq',
        python_callable=upload_data
    )

