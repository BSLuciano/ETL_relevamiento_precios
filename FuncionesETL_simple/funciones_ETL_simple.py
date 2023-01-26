import pandas as pd
import numpy as np
import os
import re
import glob
from pathlib import Path
import chardet
from datetime import datetime



# EXTRACCIÓN

def importar(path):
    # Esta función importa archivos en múltiples formatos, teniendo en cuenta que si el archivo es formato xlsx 
    # puede contener varias hojas con tablas. Además, si el archivo trata sobre relevamiento de precios, crea una
    # columna en la cual indica la fecha en la que se realizo el relevamiento extrayendo el valor del nombre del
    # archivo, el objetivo de esto es tener una referencia para hacer cargas incrementales.

    data = pd.DataFrame()
    with open(path, 'rb') as f:
        result = chardet.detect(f.read()) # Obtener encoding del archivo

        # IMPORTAR ARCHIVOS

        # csv
        if Path(path).suffix == '.csv': 
            data = pd.read_csv(path, encoding=result['encoding'], sep=',', engine='python', decimal='.')

            data_texto = os.path.split(path)[1].split('.')[0] # Extraer el texto del cual se sacará la fecha (nombre de archivo)

            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
          
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    data['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                          # se hizo el relevamiento del precio
                else:
                    data = data


        # txt
        elif Path(path).suffix == '.txt': 
            data = pd.read_table(path, encoding=result['encoding'], sep='|', engine='python')
            data_texto = os.path.split(path)[1].split('.')[0] # Extraer el texto del cual se sacará la fecha
           
            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
          
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    data['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                          # se hizo el relevamiento del precio
                else:
                    data = data

            
        # json
        elif Path(path).suffix == '.json': 
            data = pd.read_json(path, encoding=result['encoding'], precise_float=True)
            data_texto = os.path.split(path)[1].split('.')[0] # Extraer el texto del cual se sacará la fecha
            
            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
          
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    data['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                          # se hizo el relevamiento del precio
                else:
                    data = data


        # parquet
        elif Path(path).suffix == '.parquet': 

            data = pd.read_parquet(path, engine='pyarrow')
            data_texto = os.path.split(path)[1].split('.')[0].split('.')[0] # Extraer el texto del cual se sacará la fecha

            auxiliar = data_texto.split('_') # Secciono el nombre del archivo
            auxiliar = list(set(auxiliar))  # Creo una lista con los elementos únicos del nombre del archivo

            for e in auxiliar:
          
                if e.isdigit() == True: # Determino si en el nombre del archivo hay dígitos referentes a la fecha  
                    data['fecha_semana'] = datetime.strptime(e, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                          # se hizo el relevamiento del precio
                else:
                    data = data



        # xlsx
        elif Path(path).suffix == '.xlsx':
            dict_aux = pd.read_excel(path, sheet_name=None) # Diccionario donde key: nombre de hoja , values: df con valores del archivo
            lst_df = []
            for key in dict_aux.keys():                     # Agrego los df a una lista

                data_fecha = key.split('_')[-1] # fecha semana de relevamiento
                
                if data_fecha in key:

                    df = dict_aux[key]
                    df['fecha_semana'] = datetime.strptime(data_fecha, '%Y%m%d') # Creo la columna "fecha_semana" para tener la referencia de cuando
                                                                                 # se hizo el relevamiento del precio
                    lst_df.append(df)

                else:
                    df = dict_aux[key]
                    lst_df.append(df)

            data = pd.concat(lst_df, axis=0)   # Concateno los df que contiene la lista
                
            for col in data.columns:
                if 'fecha_semana' in col:
                    data = data.sort_values('fecha_semana')

            data.reset_index(drop=True, inplace = True)
                    
            
    return data



# TRANSFORMACIÓN

# El parámetro a pasar a la función es un diccionario de dataframes

def transformacion(dicc_df):

    # REGISTROS DUPLICADOS:

    # Bucle para eliminar los resgistros duplicados
    for key in dicc_df.keys():   
        dicc_df[key].drop_duplicates(inplace = True)


    # VALORES FALTANTES:

    # Bucle para eliminar las columnas categoria del dataframe producto
    for key in dicc_df.keys():
        if 'producto' in key:
            for col in dicc_df[key].columns:
                if 'categoria' in col:
                    dicc_df[key].drop(columns = col, inplace = True)

    # Hay datos de precios con valores iguales a '' los cuales deberán reemplazarse por np.nan para ser tratados como los demás valores faltantes
    # Reemplazo los valores '' por np.nan 
    for key in dicc_df.keys():
        if 'precio' in key:
            for col in dicc_df[key].columns:
                if 'precio' in col:
                    dicc_df[key][col].replace('', np.nan, inplace = True)

    # Bucle para eliminar los registros con valores faltantes
    for key in dicc_df.keys():
        dicc_df[key].dropna(inplace = True)

    
    # NORMALIZACIÓN

    # Reemplazo id por producto_id en el df producto
    for key in dicc_df.keys():
        if 'producto' in key:
            for col in dicc_df[key].columns:
                if col == 'id':
                    dicc_df[key].rename(columns = {col:'producto_id'}, inplace = True)

    # Reemplazo id por sucursal_id en el df sucursal
    for key in dicc_df.keys():
        if 'sucursal' in key:
            for col in dicc_df[key].columns:
                if col == 'id':
                    dicc_df[key].rename(columns = {col:'sucursal_id'}, inplace = True)

    # Doy el orden correcto a las columnas de los DF de precios_semana
    for key in dicc_df.keys():
        if 'precio' in key:
            dicc_df[key] = dicc_df[key][['precio', 'producto_id', 'sucursal_id', 'fecha_semana']]

    # Cambio el tipo de dato a las columnas 'precio' a float
    for key in dicc_df.keys():
        if 'precio' in key:
            for col in dicc_df[key].columns:
                if 'precio' in col:
                    dicc_df[key][col] = dicc_df[key][col].astype(float)

    
    # Función para corregir errores en los id y poner el tipo de dato correcto
    def mod_id_prod(x):
        if isinstance(x,str):
            x = x.split('-')[-1]
        elif isinstance(x,float):
            x = int(x)
        else:    
            x=x
        return str(x).zfill(13) # Esta línea es para que los id queden con el formato de código EAN de 13 dígitos

    for key in dicc_df.keys():
        if 'precios_semana' in key or 'producto' in key:
            for col in dicc_df[key].columns:
                if col == 'producto_id':
                    dicc_df[key][col] = dicc_df[key][col].apply(mod_id_prod)

    
    # Función para corregir los elementos de la columna 'sucursal_id'
    # de los DF 'precios_semana' que presentan formato 'datetime'
    def mod_suc_id(x):
        if type(x) == datetime:
            x=x.strftime("%#d-%#m-%Y")
        return x

    for key in dicc_df.keys():
        if 'precio' in key:
            for col in dicc_df[key].columns:
                if 'sucursal_id' in col:
                    dicc_df[key][col] = dicc_df[key][col].apply(mod_suc_id)


    # Bucle para eliminar outliers de precios
    for key in dicc_df.keys():
        if 'precios_semana' in key:
            for col in dicc_df[key].columns:
                if 'precio' in col:
                    dicc_df[key][col] = dicc_df[key][col][dicc_df[key][col] <= 1000000]


    # REGISTROS DUPLICADOS:

    # Bucle para eliminar los resgistros duplicados
    for key in dicc_df.keys():   
        dicc_df[key].drop_duplicates(inplace = True)

    
    # Elimino id duplicados de los DF 'producto' y 'sucursal'
    # debido a que serán primary key en la base de datos
    for key in dicc_df.keys():
        if 'producto' in key or 'sucursal' in key:
            for col in dicc_df[key].columns:
                if 'producto_id' in col or 'sucursal_id' in col:

                    dicc_df[key] = dicc_df[key].drop_duplicates(
                                    dicc_df[key].columns[dicc_df[key].columns.isin([col])],
                                    keep='first'
                                    )

    return dicc_df
