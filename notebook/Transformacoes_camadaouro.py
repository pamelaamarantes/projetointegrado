# Databricks notebook source
from pyspark.sql import SparkSession

# Inicialize a sessão do Spark
spark = SparkSession.builder \
    .appName("Leitura de DataFrame Parquet") \
    .getOrCreate()


caminho_arquivo_parquet = "dbfs:/mnt/dados/silver/merged_wines.parquet"

# Carregar o DataFrame a partir do arquivo Parquet
merged_wines_df = spark.read.parquet(caminho_arquivo_parquet)

display(merged_wines_df)




# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col

# Filtrando as 5 preferências de vinho mais vendidas para mulheres e homens
wine_preferences_top5_female = merged_wines_df.filter(col('Gender') == 'female').groupby('FavoriteWine').count().orderBy('count', ascending=False).limit(5)
wine_preferences_top5_male = merged_wines_df.filter(col('Gender') == 'male').groupby('FavoriteWine').count().orderBy('count', ascending=False).limit(5)

# Convertendo os resultados para DataFrames pandas
df_top5_female = wine_preferences_top5_female.toPandas()
df_top5_male = wine_preferences_top5_male.toPandas()

# Plotando os gráficos de barras para as preferências de vinho mais vendidas para mulheres
plt.figure(figsize=(10, 6))
plt.bar(df_top5_female['FavoriteWine'], df_top5_female['count'], color='purple')
plt.title('Top 5 Preferências de Vinho para Mulheres')
plt.xlabel('Vinho Favorito')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()


display(plt.show())

# Plotando os gráficos de barras para as preferências de vinho mais vendidas para homens
plt.figure(figsize=(10, 6))
plt.bar(df_top5_male['FavoriteWine'], df_top5_male['count'], color='blue')
plt.title('Top 5 Preferências de Vinho para Homens')
plt.xlabel('Vinho Favorito')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

display(plt.show())


# COMMAND ----------



import seaborn as sns

# Filtrando os dados para homens e mulheres
df_male = merged_wines_df.filter(col('Gender') == 'male')
df_female = merged_wines_df.filter(col('Gender') == 'female')

# Agrupando os dados por faixa etária e contando as ocorrências dos cinco vinhos mais populares para homens e mulheres
top5_wines_male = df_male.groupBy('Age Group', 'FavoriteWine').count().orderBy('count', ascending=False).limit(5)
top5_wines_female = df_female.groupBy('Age Group', 'FavoriteWine').count().orderBy('count', ascending=False).limit(5)

# Convertendo os resultados para DataFrames pandas
df_top5_wines_male = top5_wines_male.toPandas()
df_top5_wines_female = top5_wines_female.toPandas()

# Plotando gráficos de barras para os cinco vinhos mais populares por faixa etária para homens
plt.figure(figsize=(15, 8))
sns.set_palette("husl")  # Definindo a paleta de cores
for age_group in df_top5_wines_male['Age Group'].unique():
    df_age_group = df_top5_wines_male[df_top5_wines_male['Age Group'] == age_group]
    plt.bar(df_age_group['FavoriteWine'], df_age_group['count'], alpha=0.7, label=age_group)

plt.title('Top 5 Preferências de Vinho para Homens por Faixa Etária')
plt.xlabel('Vinho Favorito')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')
plt.legend(title='Faixa Etária')
plt.tight_layout()
plt.show()

# Plotando gráficos de barras para os cinco vinhos mais populares por faixa etária para mulheres
plt.figure(figsize=(15, 8))
sns.set_palette("husl")  # Definindo a paleta de cores
for age_group in df_top5_wines_female['Age Group'].unique():
    df_age_group = df_top5_wines_female[df_top5_wines_female['Age Group'] == age_group]
    plt.bar(df_age_group['FavoriteWine'], df_age_group['count'], alpha=0.7, label=age_group)

plt.title('Top 5 Preferências de Vinho para Mulheres por Faixa Etária')
plt.xlabel('Vinho Favorito')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')
plt.legend(title='Faixa Etária')
plt.tight_layout()
plt.show()

