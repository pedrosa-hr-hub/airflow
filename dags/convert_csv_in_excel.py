from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from openpyxl import Workbook
import csv


def transfrom():

    wb = Workbook()
    ws = wb.active

    with open('/opt/airflow/data/operations.csv', 'r') as f:
        for row in csv.reader(f):
            ws.append(row)
    wb.save('/opt/airflow/data/test.xlsx')

    return "Success"


with DAG('convert_csv_in_excel', start_date=datetime(2023, 3, 2),
         schedule_interval='30 * * * *',
         dagrun_timeout=timedelta(minutes=60),
         tags=['convert_file'],
         catchup=False
         ) as dag:

    t1 = PythonOperator(
        task_id="main",
        python_callable=transfrom
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3,
        dag=dag
    )

# Fila de Task
t1 >> t2
