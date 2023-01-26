import sqlalchemy as db

# Por medio de SQLAlchemy realizo la conexión entre python y la base de datos de PostgreSQL

database_username='airflow' # Nombre de cliente en PostgreSQL
database_password='airflow' # Contraseña de PostgreSQL
database_ip='localhost:5432'
database_name='relevamiento_precios' # Nombre de Base de datos a donde nos conectaremos


database_conection=db.create_engine(f'postgresql://{database_username}:{database_password}@{database_ip}/{database_name}')
conection=database_conection.connect()
metadata=db.MetaData()