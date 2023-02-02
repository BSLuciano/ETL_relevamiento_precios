
# Le indica a Docker que se quiere extender la imagen oficial de Airflow
FROM apache/airflow:2.5.0

# Copiar el archivo requirements.txt en la imagen
COPY requirements-v2.txt requirements.txt

# Ejecutar el comando de actualización de pip para obtener su última versión
# Ejecutar la instalación de pip para instalar todas dependencias de python
RUN pip install --user --upgrade pip

RUN pip install --no-cache-dir --user -r requirements.txt