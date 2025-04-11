#!/usr/bin/env python
# coding: utf-8
import os


'''activate_env = os.path.join(os.getcwd(), 'venv', 'Scripts', 'activate_this.py')


exec(open(activate_env).read(), {'__file__': activate_env})
'''
# Imports
import subprocess
import polars as pl
import connectorx as cx
from datetime import date


QUERY_FILE_CENSUS = "queryDemo.sql"
HOST = 'localhost'
DATABASE = 'mimiciv'




dirname = os.path.dirname(__file__)
query_path = os.path.join(dirname, "..", "1.query", QUERY_FILE_CENSUS)
output_path = os.path.join(dirname, "..", '2.rawDataset', 'census')

# ### SQL Connexion 

conn_uri = f'postgresql://postgres:password@{HOST}/{DATABASE}'

query_census = open(query_path).read()

print('Chargement requêtes SQL : OK')


# ## Load Dataframe

raw_data = pl.read_database_uri(query_census, conn_uri, engine='connectorx')

print('Chargement du dataset : OK')

# STORE DATASET

OUTPUT_PARQUET_NAME = 'raw_census_dataset_' + str(date.today()) + '.parquet'
OUTPUT_DESCRIPTION_NAME = 'raw_census_description_' + str(date.today()) + '.csv'

# dataset description
raw_data.write_parquet(os.path.join(output_path, OUTPUT_PARQUET_NAME))

print(f'Enregistrement du fichier {OUTPUT_PARQUET_NAME} : OK')

# dataset description
raw_data.describe().write_csv(os.path.join(output_path, OUTPUT_DESCRIPTION_NAME))
print(f'Enregistrement de la descrption : OK')

