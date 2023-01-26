from io import BytesIO, StringIO
import pandas as pd
from minio import Minio
import pyarrow.parquet as pq
import pyarrow as pa
import os


host = os.environ.get('HOST')
access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')
bucket = os.environ.get('BUCKET')


# Función para cargar archivos en MinIO
def Load_MinIO(bucket, df, key):

    df = df
    key = key

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
def df_minio(bucket, filename):
    minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    obj = minioClient.get_object(bucket, f'/{filename}')
    data = obj.data
    data = BytesIO(data)
    df = pd.read_parquet(data, engine='pyarrow')

    return df


# Extrae los archivos alojados en un bucket de MinIO y crea un diccionario de df con ellos
# asignando como clave el nombre de archivo
def minio_download(bucket):
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
        dicc_df[filename] = df.head()
    
    return dicc_df

