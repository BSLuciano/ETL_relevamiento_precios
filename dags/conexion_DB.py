import sqlalchemy as db

# Por medio de SQLAlchemy realizo la conexión entre python y la base de datos de PostgreSQL

access_key='airflow' # Nombre de cliente en PostgreSQL
secret_key='airflow' # Contraseña de PostgreSQL
host='host.docker.internal:5432' 
database='relevamiento_precios' # Nombre de Base de datos a donde nos conectaremos


database_conection=db.create_engine(f'postgresql+psycopg2://{access_key}:{secret_key}@{host}/{database}')
conection=database_conection.connect()
metadata=db.MetaData()