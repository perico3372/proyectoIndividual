#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 06:45:17 2023

@author: pablo
"""
from fastapi import FastAPI
app = FastAPI()
import os
import pandas as pd

#movie_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/movie.csv"
#country_file = "country.csv"
#country_file ="https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/country.csv"
#production_file = "production.csv"
#production_file = "https://raw.githubusercontent.com/perico3372/proyectoIndividual/main/production.csv"

#dataframe_movie = pd.read_csv(movie_file)#, usecols=range(11))
#dataframe_production =  pd.read_csv(production_file)#, usecols=range(11))
#dataframe_country =  pd.read_csv(country_file)#, usecols=range(11))

@app.get("/")
def mensaje():
  return{"Hello": "World"}
