#!/usr/bin/env python
# coding: utf-8
import requests

url2 = 'https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/titles.parquet'

url1 = 'https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/ratings.parquet'
#url1 = 'https://github.com/perico3372/proyectoIndividual/blob/main/ratings.parquet'

#url2 = 'https://github.com/perico3372/proyectoIndividual/blob/main/titles.parquet'

response1 = requests.get(url1)
response2 = requests.get(url2)

if response1.status_code == 200 and response2.status_code == 200:
    data1 = response1.text
    data2 = response2.text
    # Aquí puedes hacer lo que quieras con los datos de los archivos
else:
    print("No se pudo acceder a los archivos")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import explode
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.getOrCreate()

dataFrameTitlesFinal = spark.read.parquet(url2)

unionDataFramePromedioScored = spark.read.parquet(url1)


from fastapi import FastAPI

app = FastAPI()

'''
1 - Película (sólo película, no serie, etc) con mayor duración según año, 
plataforma y tipo de duración. La función debe llamarse get_max_duration(year, 
platform, duration_type) y debe devolver sólo el string del nombre de la 
película.
'''

from pyspark.sql.functions import max

@app.get('/get_max_duration/{anio}/{platform}/{duration_type}')

def get_max_duration(platform:str, anio:int, duration_type:str):

    filtro = dataFrameTitlesFinal.filter((col('platform') == platform) & (col('release_year') == anio) & (col('duration_type') == duration_type))

    maxDuracion = filtro.agg(max(col('duration_int'))).collect()[0][0]
    maxDuracionPelicula = filtro.filter(col('duration_int') == maxDuracion).select('title').collect()[0][0]

    return {'pelicula': maxDuracionPelicula}

#print(get_max_duration('netflix', 2016, 'min'))

#print(get_max_duration('netflix', 2017, 'min'))

'''
2 - Cantidad de películas (sólo películas, no series, etc) según plataforma, 
con un puntaje mayor a XX en determinado año. La función debe llamarse 
get_score_count(platform, scored, year) y debe devolver un int, con el total 
de películas que cumplen lo solicitado.
'''

@app.get('/get_score_count/{platform}/{scored}/{anio}')

def get_score_count(platform:str, scored:float, anio:int):
    
    '''
    Filtrar el DataFrame para obtener únicamente películas con la plataforma, 
    el año y el puntaje indicados
    '''
    
    filtro= unionDataFramePromedioScored.filter((col('platform') == platform) & (col('type') == 'movie') & (col('promedio_scored') > scored) & (col('release_year') == anio))
    cantidadDos = filtro.count()
    
    return {'plataforma': platform, 'cantidad': cantidadDos, 'anio': anio, 'score': scored}

#print(get_score_count('netflix', 3.0, 2015))

'''
3 - Cantidad de películas (sólo películas, no series, etc) según plataforma. 
La función debe llamarse get_count_platform(platform) y debe devolver un int, 
con el número total de películas de esa plataforma. Las plataformas deben 
llamarse amazon, netflix, hulu, disney.
'''

@app.get('/get_count_platform/{platform}')

#from pyspark.sql.functions import col

def get_count_platform(platform:str):
    # Filtrar DataFrame para obtener solo películas
    pelicula = dataFrameTitlesFinal.filter(col('type') == 'movie')

    # Filtrar por plataforma
    platforma = col('platform') == platform
    platformaPelicula = pelicula.filter(platforma)

    # Contar cantidad de películas por plataforma
    CantidadTres = platformaPelicula.count()

    
    return {'plataforma': platform, 'peliculas': CantidadTres}

#print(get_count_platform('amazon'))

#print(get_count_platform('disney'))

#print(get_count_platform('netflix'))

#print(get_count_platform('hulu'))

'''
4 - Actor que más se repite según plataforma y año. La función debe llamarse 
get_actor(platform, year) y debe devolver sólo el string con el nombre del 
actor que más se repite según la plataforma y el año dado.
'''
@app.get('/get_actor/{platform}/{anio}')



def get_actor(platform:str, anio:int):
    # Filtrar el DataFrame para obtener las filas correspondientes a la plataforma y el año dado
    filtroCuatro = dataFrameTitlesFinal.filter((col('platform') == platform) & (col('release_year') == anio))

    # Separar los datos por coma en la columna "cast"
    listaActores = filtroCuatro.select('cast').rdd.flatMap(lambda x: x[0].split(','))
    listaActores = spark.createDataFrame(listaActores, StringType())
    listaActores = listaActores.select(explode(split('value', ',')).alias('actor')).filter(col('actor') != '')

    # Obtener el actor más común
    actorMasComun = listaActores.groupBy('actor').count().orderBy('count', ascending=False).first().actor
    #cantidadApariciones = actorMasComun.count()
    # Devolver el actor más común
    return {'plataforma': platform, 'anio': anio, 'actor': actorMasComun} #'hla' :cantidadApariciones}

#print(get_actor('netflix', 2018))

'''
5 - La cantidad de contenidos/productos (todo lo disponible en streaming) que 
se publicó por país y año. La función debe llamarse prod_per_county(tipo,pais
anio) deberia devolver la cantidada de contenidos/productos segun el tipo de 
contenido (pelicula,serie) por pais y año en un diccionario con las variables 
llamadas 'pais' (nombre del pais), 'anio' (año), 'pelicula' (cantidad de 
contenidos/productos).    
'''

from pyspark.sql.functions import col

@app.get('/prod_per_county/{tipo}/{pais}/{anio}')

def prod_per_county(tipo:str, pais:str, anio:int):
    # Seleccionar el tipo de contenido y filtrar por país y año
    filtroCinco = dataFrameTitlesFinal.filter((col('type') == tipo) & (col('country') == pais) & (col('release_year') == anio))
    
    # Obtener la cantidad de contenidos/productos publicados
    cantidadContenido = filtroCinco.count()

    return {'pais': pais, 'anio': anio, 'peliculas': cantidadContenido}

#print(prod_per_county('movie', 'united states', 2019))

'''
6 - La cantidad total de contenidos/productos (todo lo disponible en streaming, 
series, peliculas, etc) según el rating de audiencia dado (para que publico 
fue clasificada la pelicula). La función debe llamarse get_contents(rating) y 
debe devolver el numero total de contenido con ese rating de audiencias.
'''

@app.get('/get_contents/{rating}')

def get_contents(rating:str):
    # Filtrar el DataFrame para obtener solo los contenidos con el rating de audiencia indicado
    filtroSeis = dataFrameTitlesFinal.filter(col('rating') == rating)
    
    return {'rating': rating, 'contenido': filtroSeis.count()}

#print(get_contents('g'))

#print(get_contents('r'))

#print(get_contents('13+'))





