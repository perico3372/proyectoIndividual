#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 06:45:17 2023

@author: pablo
"""

import os
import pandas as pd
os.chdir('/_disk_misc/api')
from fastapi import FastAPI
app = FastAPI()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import avg
from pyspark.sql.functions import when
from pyspark.sql.functions import sum
from pyspark.sql.functions import to_date
from pyspark.sql.functions import asc

spark = SparkSession.builder.getOrCreate()

#archivo = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/data_00.csv"
#movie_file = "movie.csv"
movie_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/movie.csv"
#country_file = "country.csv"
country_file ="https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/country.csv"
#production_file = "production.csv"
production_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/production.csv"

dataframe_movie_pandas = pd.read_csv(movie_file)#, usecols=range(11))
dataframe_production_pandas =  pd.read_csv(production_file)#, usecols=range(11))
dataframe_country_pandas =  pd.read_csv(country_file)#, usecols=range(11))

dataframe_movie = spark.createDataFrame(dataframe_movie_pandas)
dataframe_production = spark.createDataFrame(dataframe_production_pandas)
dataframe_country = spark.createDataFrame(dataframe_country_pandas)


@app.get("/mes")
def peliculas_month(month:str):
    query_one = dataframe_movie.filter(dataframe_movie.month == month).count()
    return {"Cantidad de peliculas en el month de": month,  "fue de": query_one}

#print(peliculas_month("Febrero"))

@app.get("/dia")
def peliculas_month(day:str):
    '''Ingresar día de la semana retornará cantidad de peliculas estrenadas ese día de la semana
    Nota: debera ingresar el día con la primera en mayuscula (Lunes) por ejemplo'''
    query_two = dataframe_movie.filter(col("day_week") == day).count()
    return {"Cantidad de peliculas en el day de la semana de": day,  "fue de": query_two}
#print(peliculas_month("Jueves"))

@app.get("/franquicia")
def franquicia(franquicia):
    '''Se ingresar la franquicia, retornando la cantidad de peliculas, ganancia total y la ganacia promedio'''
    query_three = dataframe_movie.filter(col("collection") == franquicia)
    count_movie = query_three.count()
    query_profit_collection_sum = query_three.agg(sum(col("profit"))).collect()[0][0]
    query_profit_collection_avg = query_three.agg(avg(col("profit"))).collect()[0][0]
    return {'franquicia':franquicia, 'cantidad':count_movie, 'ganancia_total':query_profit_collection_sum, 'ganancia_promedio':query_profit_collection_avg}
#print(franquicia("Toy Story Collection"))

@app.get("/pais")
def peliculas_pais(pais):
    '''Ingrese el pais, retornando la cantidad de peliculas producidas por el pais'''
    group_country= dataframe_country.filter(col("country") == pais)
    query_four = group_country.count()
    return {'pais': pais, 'cantidad': query_four}
#peliculas_pais("Germany")   
#print(peliculas_pais("United States of America"))

@app.get("/productora")
def productoras(productora):
    '''Ingresas la productora, retornando la ganancia toal y la cantidad de peliculas que produjeron'''
    production_companies = dataframe_production.filter(dataframe_production["companies"] == productora)
    count_movie = production_companies.count()
    profit_sum = production_companies.agg(sum(col("profit"))).collect()[0][0]
    return {'productora':productora, 'ganancia_total':profit_sum, 'cantidad':count_movie}    
#print(productoras("Pixar Animation Studios"))
 
def retorno(pelicula):
    '''Ingresas la pelicula, retornando la inversion, la ganancia, el retorno y el año en el que se lanzo'''
    group_movie = dataframe_movie.filter(col("title") == pelicula)
    query_budget = group_movie.select(col("budget")).collect()[0][0]
    query_profit = group_movie.select(col("profit")).collect()[0][0]   
    query_return = group_movie.select(col("return")).collect()[0][0]
    query_year = group_movie.select(col("year")).collect()[0][0]    
    return {'pelicula':pelicula, 'inversion':query_budget, 'ganancia':query_profit,'retorno':query_return, 'anio':query_year}
#print(retorno("Toy Story"))






