from datetime import datetime, timedelta
from funciones_ETL_airflow import *
from funciones_MinIO import *


from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    "owner": "LBS",
    "retries": 5,
    "retry_delay": timedelta(minutes=10)
}


def get_fecha_semana():
    hook = S3Hook('minio_conn')
    files = hook.list_keys('t-carga-incremental')
    filename = files[-1].split('.')[0]
    auxiliar = filename.split('_') # Secciono el nombre del archivo
    auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

    fecha_semana = ''
    for e in auxiliar:
        if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha
            fecha_semana = datetime.strptime(e, '%Y%m%d')
    return fecha_semana

fecha_semana = get_fecha_semana()


with DAG(
    default_args=default_args,
    dag_id="ETL_carga_incremental",
    start_date=datetime(2022, 5, 19),
    schedule_interval="@daily"
) as dag:

    # Controla si se ha cargado un archivo que contenga la palabra precio en su nombre
    check = S3KeySensor(
        task_id='sensor_minio_S3',
        bucket_name='o-carga-incremental',
        bucket_key='precio*',
        wildcard_match = True, # Activando wildcard habilito el * para la busqueda de claves incompletas
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )

    # Extracción y Transformación 

    task_0 = EmptyOperator(
        task_id = 'ETL_inicio',
        dag = dag
    )

    
    task_1 = PythonOperator(
        task_id = 'ET_precios',
        python_callable = transform_precios_CI,
        op_kwargs={
            'bucket_extract': 'o-carga-incremental',
            'bucket_load': 't-carga-incremental'
            }
    )

    task_2 = EmptyOperator(
        task_id = 'T_fin',
        dag = dag
    )

# Carga

    # En Airflow es recomendable eliminar los datos antes de insertarlos para evitar la duplicación de datos
    # o la violación de la clave primaria.
    task_3 = PostgresOperator(
        task_id='delete_data_DB',
        postgres_conn_id='postgres_conn',
        parameters={'fecha_semana':fecha_semana},
        sql="""
            delete from precio where fecha_semana = %(fecha_semana)s
        """ 
    )

    task_4 = PythonOperator(
        task_id = 'L_PostgreSQL',
        python_callable = load_CI,
        op_kwargs={'bucket': 't-carga-incremental'}
    )

    task_5 = EmptyOperator(
        task_id = 'ETL_fin',
        dag = dag
    )


check >> task_0 >> task_1 >> task_2 >> task_3 >> task_4 >> task_5