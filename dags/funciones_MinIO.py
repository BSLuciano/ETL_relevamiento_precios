from io import BytesIO, StringIO
import pandas as pd
from minio import Minio
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import chardet
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


host = 'host.docker.internal:9000'
access_key = 'ROOTUSER'
secret_key = 'CHANGEME123'


# Función para cargar archivos en MinIO
def Load_MinIO(bucket: str, df, key):

    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)

    found = minioClient.bucket_exists(bucket)
    if not found:
        minioClient.make_bucket(bucket)
    
    else:
        print("Bucket already exists")

    parquet_bytes = df.to_parquet() # .encode("utf-8")
    parquet_buffer = BytesIO(parquet_bytes)

    minioClient.put_object( bucket, # nombre del contenedor en MinIO
                            f"/t_{key}.parquet", # nombre que tendrá el archivo con los datos transformado en MinIO
                            data=parquet_buffer,
                            length=len(parquet_bytes),
                            content_type='application/parquet'
                            )


# Crea un df al pasar el nombre de bucket y archivo en MinIO
def df_minio(bucket: str, filename):
    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    obj = minioClient.get_object(bucket, f'/{filename}')
    data = obj.data
    data = BytesIO(data)
    df = pd.read_parquet(data, engine='pyarrow')
    obj.close()
    obj.release_conn()
    return df


# Funciones de extracción de objetos de MinIO:
# Extraen los archivos alojados en un bucket de MinIO y crea un diccionario de df con ellos
# asignando como clave el nombre de archivo
# v1: está realizada con funciones del módulo Minio de Python
# v2: está realizada utilizando S3Hook de los módulos de Airflow provistos por Amazon

def get_data_minio_v1(bucket: str):
    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    # Lista con los nombres de los objetos que se encuentrar en el bucket de MinIo
    lst_filename = []
    for obj in minioClient.list_objects(bucket):
        filename = obj.object_name
        lst_filename.append(filename)
    # Crea el diccionario de df
    dicc_df = {}
    for filename in lst_filename:
        df = df_minio(bucket, filename)
        dicc_df[filename] = df
    
    return dicc_df



# ______________________________________________________________________________________________________ #



# CARGA INCREMENTAL


def df_minio_CI(bucket, filename):
    
    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    obj = minioClient.get_object(bucket, f'/{filename}')
    data = obj.data

    with BytesIO(data) as f:
        result = chardet.detect(str(f).encode()) # Obtener encoding del archivo

        df = pd.DataFrame()

        # csv
        if '.csv' in filename:
            df = pd.read_csv(BytesIO(data), encoding=result['encoding'], sep=',', engine='python', decimal='.')

            data_texto = filename.split('.')[0] # Extraer el texto del cual se sacará la fecha (nombre de archivo)

            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
        
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    df['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                        # se hizo el relevamiento del precio
                else:
                    df = df

        # txt
        elif  '.txt' in filename: 
            df = pd.read_table(BytesIO(data), encoding=result['encoding'], sep='|', engine='python')
            data_texto = filename.split('.')[0] # Extraer el texto del cual se sacará la fecha
            
            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
            
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    df['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                            # se hizo el relevamiento del precio
                else:
                    df = df

        # json
        elif '.json' in filename:

            df = pd.read_json(BytesIO(data), encoding=result['encoding'], precise_float=True)
            data_texto = filename[1].split('.')[0] # Extraer el texto del cual se sacará la fecha
            
            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
            
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    df['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                            # se hizo el relevamiento del precio
                else:
                    df = df

        # parquet
        elif '.parquet' in filename: 

            df = pd.read_parquet(BytesIO(data), engine='pyarrow')
            data_texto = filename.split('.')[0].split('.')[0] # Extraer el texto del cual se sacará la fecha

            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
            
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    df['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                            # se hizo el relevamiento del precio
                else:
                    df = df

        # xlsx
        elif '.xlsx' in filename:
            dict_aux = pd.read_excel(BytesIO(data), sheet_name=None) # Diccionario donde key: nombre de hoja , values: df con valores del archivo
            lst_df = []
            for key in dict_aux.keys():                     # Agrego los df a una lista

                data_fecha = key.split('_')[-1] # fecha semana de relevamiento
                
                if data_fecha in key:

                    df_aux = dict_aux[key]
                    df_aux['fecha_semana'] = datetime.strptime(data_fecha, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                                 # se hizo el relevamiento del precio
                    lst_df.append(df_aux)

                else:
                    df_aux = dict_aux[key]
                    lst_df.append(df_aux)

            df = pd.concat(lst_df, axis=0)   # Concateno los df que contiene la lista
                
            for col in df.columns:
                if 'fecha_semana' in col:
                    df = df.sort_values('fecha_semana')

            df.reset_index(drop=True, inplace = True)
                    
    obj.close()
    obj.release_conn()
    return df


def get_data_minio_v2(bucket):
    dicc_df = {}

    hook = S3Hook('minio_conn')
    files = hook.list_keys(bucket)
    key = files[-1]
    
    # filename = hook.get_key(key, bucket)
    df = df_minio_CI(bucket, key)
    dicc_df[key] = df

    return dicc_df


def Load_MinIO_CI(bucket, df, key):

    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)

    found = minioClient.bucket_exists(bucket = bucket)
    if not found:
        minioClient.make_bucket(bucket = bucket)
    
    else:
        print("Bucket already exists")

    parquet_bytes = df.to_parquet() # .encode("utf-8")
    parquet_buffer = BytesIO(parquet_bytes)

    minioClient.put_object( bucket, # nombre del contenedor en MinIO
                            f"/carga_incremental/t_{key}.parquet", # nombre que tendrá el archivo con los datos transformado en MinIO
                            data=parquet_buffer,
                            length=len(parquet_bytes),
                            content_type='application/parquet'
                            )
