# Databricks notebook source
dbutils.fs.ls("/mnt/dados/Inbound")

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace ,substring
from pyspark.sql.types import IntegerType, FloatType

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("WineAnalysis").getOrCreate()


# COMMAND ----------

# Caminhos dos arquivos
wines_path = 'dbfs:/mnt/dados/Inbound/Wines.xlsx'
people_path = 'dbfs:/mnt/dados/Inbound/people.csv'

# Carregar o arquivo Wines.xlsx
wines_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(wines_path)

# Carregar o arquivo people.csv
people_df = spark.read.csv(people_path, header=True, inferSchema=True)

# Função para obter dados da API
def get_api_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None

# URL da API
api_url = "https://dhpftp1i99.execute-api.us-east-1.amazonaws.com/aipwine"

# Obtendo dados da API
api_data = get_api_data(api_url)
api_body = json.loads(api_data['body'])
api_df = spark.createDataFrame([api_body])


# COMMAND ----------

display(wines_df)

# COMMAND ----------

# MAGIC %md
# MAGIC TRATAMENTOS INICIAIS

# COMMAND ----------

# Substituir valores vazios nas colunas
wines_df = wines_df.na.fill({
    'Vintage': 'Unknown',
    'Country': 'Unknown',
    'County': 'Unknown',
    'Designation': 'Unknown',
    'Points': 0,
    'Price': 0,
    'Province': 'Unknown',
    'Title': 'Unknown',
    'Imagem': 'Unknown',
    'Variety': 'Unknown',
    'Winery': 'Unknown'
})


# COMMAND ----------

display(wines_df)

# COMMAND ----------

# Extrair o ano da coluna "Vintage" e criar uma nova coluna "Year"
wines_df = wines_df.withColumn("Year", substring(col("Vintage"), 1, 4).cast(IntegerType()))


# COMMAND ----------

display(wines_df.head(10))

# COMMAND ----------

# Convertendo "Price" para float e tirando o $

wines_df = wines_df.withColumn("Price", regexp_replace(col("Price"), "[$,]", "").cast(FloatType()))

# COMMAND ----------

display(wines_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC UNINDO API, WINES e PEOPLE

# COMMAND ----------

# Juntando os dados da API com o arquivo de vinhos com a coluna ID
merged_df = wines_df.join(api_df, on="id", how="left")

#uma junção "left", o que significa que todas as linhas  serão mantidas no resultado final


# Juntando os dados de pessoas com o arquivo de vinhos (FavoriteWine* com Variety*)
final_df = merged_df.join()



# COMMAND ----------

display(merged_df)

