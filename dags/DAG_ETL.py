from datetime import datetime, timedelta
from funciones_ETL_airflow import *
from funciones_MinIO import *

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner' : 'LBS',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=10)
}


with DAG(
    default_args = default_args,
    dag_id = 'ETL_0',
    description = 'ETL | Carga inicial',
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

# Extracción y Transformación 

    task_0 = EmptyOperator(
        task_id = 'ETL_inicio',
        dag = dag
    )

    task_1 = PythonOperator(
        task_id = 'ET_sucursal',
        python_callable = transform_sucursal,
        op_kwargs= {'bucket': 't-carga-inicial'}
    )

    task_2 = PythonOperator(
        task_id = 'ET_producto',
        python_callable = transform_producto,
        op_kwargs= {'bucket': 't-carga-inicial'}
    )

    task_3 = PythonOperator(
        task_id = 'ET_precios',
        python_callable = transform_precios,
        op_kwargs= {'bucket': 't-carga-inicial'}
    )

    task_4 = EmptyOperator(
        task_id = 'T_fin',
        dag = dag
    )

# Carga

    task_5 = PythonOperator(
        task_id = 'L_PostgreSQL',
        python_callable = load,
        op_kwargs= {'bucket': 't-carga-inicial'}
    )

    task_6 = EmptyOperator(
        task_id = 'ETL_fin',
        dag = dag
    )



    task_0 >> (task_1, task_2, task_3) >> task_4 >> task_5 >> task_6