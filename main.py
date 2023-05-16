#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 06:45:17 2023

@author: Pablo Perez
"""
from fastapi import FastAPI
app = FastAPI()
import os
import pandas as pd
#from sklearn.metrics.pairwise import linear_kernel
#from sklearn.feature_extraction.text import TfidfVectorizer


movie_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/movie.csv"

country_file ="https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/country.csv"

production_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/production.csv"

#recomendation_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/recomendation_.csv"

dataframe_movie = pd.read_csv(movie_file)#, usecols=range(11))
dataframe_production =  pd.read_csv(production_file)
dataframe_country =  pd.read_csv(country_file)

dataframe_recomendation = pd.read_csv(recomendation_file)
@app.get("/mes/{mes}")
def peliculas_mes(mes: str):
    movies_month = dataframe_movie[dataframe_movie['month'] == mes]
    count_month = movies_month.shape[0]
    return {"Cantidad de peliculas en el mes de": mes, "fue de": count_month}

#print(peliculas_mes("Febrero"))

@app.get("/dia/{dia}")
def peliculas_dia(dia: str):
    movies_day = dataframe_movie[dataframe_movie['day_week'] == dia]
    count_day = movies_day.shape[0]
    return {"Cantidad de peliculas en el mes de": dia, "fue de": count_day}

#print(peliculas_dia("Jueves"))

@app.get("/franquicia/{franquicia}")
def franquicia(franquicia:str):
    query_three = dataframe_movie[dataframe_movie['collection'] == franquicia]
    count_movie = query_three.shape[0]
    query_profit_collection_sum = query_three['profit'].sum()
    query_profit_collection_avg = query_three['profit'].mean()
    return {'franquicia': franquicia, 'cantidad': count_movie, 'ganancia_total': query_profit_collection_sum, 'ganancia_promedio': query_profit_collection_avg}
#print(franquicia("Toy Story Collection"))

@app.get("/pais/{pais}")
def peliculas_pais(pais:str):
    '''Ingrese el pais, retornando la cantidad de peliculas producidas por el pais'''
    filtered_df = dataframe_country[dataframe_country['country'] == pais]
    count_country = filtered_df.shape[0]
    return {'pais': pais, 'cantidad': count_country}
#print(peliculas_pais("United States of America"))

@app.get("/productora/{productora}")
def productoras(productora:str):
    '''Ingresa la productora, retornando la ganancia total y la cantidad de películas que produjeron'''
    production_companies = dataframe_production[dataframe_production["companies"] == productora]
    count_movie = production_companies.shape[0]
    profit_sum = production_companies["profit"].sum()
    return {'productora': productora, 'ganancia_total': profit_sum, 'cantidad': count_movie}
#print(productoras("Pixar Animation Studios"))

@app.get("/retorno/{pelicula}")
def retorno(pelicula:str):
    '''Ingresa la película, retornando la inversión, la ganancia, el retorno y el año en el que se lanzó'''
    group_movie = dataframe_movie[dataframe_movie["title"] == pelicula]
    query_budget = group_movie["budget"].values[0]
    query_profit = group_movie["profit"].values[0]
    query_return = group_movie["return"].values[0]
    query_year = group_movie["year"].values[0]
    return {'pelicula': pelicula, 'inversion': query_budget, 'ganancia': query_profit, 'retorno': query_return, 'anio': query_year}
#print(retorno("Toy Story"))




#print(dataframe_recomendation)






#print(recomendacion("Toy Story"))

