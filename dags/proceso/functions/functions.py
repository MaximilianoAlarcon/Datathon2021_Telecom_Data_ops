import pandas as pd
import numpy as np
import gzip
import io
import requests

def extraer_gzip_en_pandas_dataframe(url):
  web_response = requests.get(url,timeout=3000, stream=True)
  csv_gz_file = web_response.content
  f = io.BytesIO(csv_gz_file)
  with gzip.GzipFile(fileobj=f) as fh:
    reader = pd.read_csv(fh, sep='\t', na_values=r'\N')
  return reader

def imputar_faltantes_por_valor_medio(dataset, columna):
    media = dataset[columna].mean()
    dataset[columna] = dataset[columna].fillna(media)

def imputar_faltantes_por_constante(dataset, columna, constante):
  dataset[columna] = dataset[columna].fillna(constante)

def cargar_resultado_en_csv():
    df_peliculas = extraer_gzip_en_pandas_dataframe("https://datasets.imdbws.com/title.basics.tsv.gz")

    mascara = (df_peliculas["startYear"] >= 2015) & (df_peliculas["startYear"] <= 2020) & (df_peliculas["titleType"] == "movie")
    df_peliculas = df_peliculas[mascara]


    columnas_necesarias = ["tconst","startYear","runtimeMinutes","genres"]
    df_peliculas = df_peliculas[columnas_necesarias]

    df_peliculas["runtimeMinutes"] = df_peliculas["runtimeMinutes"].astype(float)
    imputar_faltantes_por_valor_medio(df_peliculas,"runtimeMinutes")
    imputar_faltantes_por_constante(df_peliculas,'genres','')

    df_peliculas["startYear"] = df_peliculas["startYear"].astype(int)
    df_peliculas["runtimeMinutes"] = round(df_peliculas["runtimeMinutes"],2)

    df_peliculas.sort_values(by="startYear",ascending=True,inplace=True)

    
    df_ratings = extraer_gzip_en_pandas_dataframe("https://datasets.imdbws.com/title.ratings.tsv.gz")
    mascara = df_ratings["tconst"].isin(df_peliculas["tconst"].values)
    df_ratings = df_ratings[mascara]


    df_peliculas = df_peliculas.merge(df_ratings, left_on='tconst', right_on='tconst')
    del df_ratings

    dic = []

    def separar_generos(x):
      explode = x[3].split(",")
      for genre in explode:
        dic.append([x[0],x[1],x[2],genre,x[4],x[5]])

    df_peliculas[["tconst","startYear","runtimeMinutes","genres","averageRating","numVotes"]].apply(separar_generos,axis=1)

    df_peliculas = pd.DataFrame(dic, columns = ["tconst","startYear","runtimeMinutes","genres","averageRating","numVotes"])

    df_peliculas = df_peliculas[["startYear","genres","runtimeMinutes","averageRating","numVotes"]]

    df_peliculas = df_peliculas.groupby(["startYear","genres"]).agg(
        {'runtimeMinutes': "mean", 'averageRating': 'mean', "numVotes": "sum"}
    )
    df_peliculas["runtimeMinutes"] = round(df_peliculas["runtimeMinutes"],2)
    df_peliculas["averageRating"] = round(df_peliculas["averageRating"],2)
    df_peliculas.reset_index(inplace=True)

    df_peliculas.to_csv("/home/airflow/resultados.csv",index=False)