import sqlalchemy as db

# Por medio de SQLAlchemy realizo la conexión entre python y la base de datos de PostgreSQL

access_key='airflow' # Nombre de cliente en PostgreSQL
secret_key='airflow' # Contraseña de PostgreSQL
host='host.docker.internal' 
database='relevamiento-precios' # Nombre de Base de datos a donde nos conectaremos


database_connection=db.create_engine(f'postgresql+psycopg2://{access_key}:{secret_key}@{host}/{database}')
connection=database_connection.connect()
metadata=db.MetaData()