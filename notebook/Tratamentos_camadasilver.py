# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

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
wines_path = 'dbfs:/mnt/dados/bronze/Wines.xlsx'
people_path = 'dbfs:/mnt/dados/bronze/people.csv'

# Carregar o arquivo Wines.xlsx
wines_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(wines_path)

# Carregar o arquivo people.csv
people_df = spark.read.csv(people_path, header=True, inferSchema=True)



# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
 
 
def fetch_api_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return json.loads(response.text)['body']
    else:
        raise Exception(f"Erro ao buscar os dados da API. Código de status: {response.status_code}")
 
# URL da API
api_url = 'https://dhpftp1i99.execute-api.us-east-1.amazonaws.com/aipwine'
 
 
spark = SparkSession.builder \
    .appName("API to DataFrame") \
    .getOrCreate()
 
 
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Purchased_wine", StringType(), True),
    StructField("Price", FloatType(), True)
])
 
# Obtendo os dados da API
api_data = fetch_api_data(api_url)
 
 
data_list = json.loads("[" + api_data.replace("},{", "},{") + "]")
 
api_df = spark.createDataFrame(data_list, schema=schema)

# COMMAND ----------

display(wines_df)

# COMMAND ----------


wines_df = wines_df.withColumnRenamed('Country', 'Country_Wine')
wines_df = wines_df.withColumnRenamed('id', 'Id_Wine')
people_df = people_df.withColumnRenamed('_c0','id')
merged_wines_df = people_df.join(wines_df, people_df["FavoriteWine"] == wines_df["Variety"], how="right")
display(merged_wines_df)


# COMMAND ----------

# MAGIC %md
# MAGIC TRATAMENTOS INICIAIS

# COMMAND ----------

# MAGIC %md
# MAGIC Removendo linhas NULL

# COMMAND ----------

# Removendo linhas com valores nulos do DataFrame
merged_wines_df = merged_wines_df.na.drop()

# Exibindo o DataFrame após a remoção dos valores nulos
display(merged_wines_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import FloatType

# Convertendo "Price" para float e tirando o $
merged_wines_df = merged_wines_df.withColumn("Price", regexp_replace(col("Price"), "[$,]", "").cast(FloatType()))

display(merged_wines_df)


# COMMAND ----------

from pyspark.sql.functions import when, substring

# Convertendo a coluna 'Age' para um formato numérico
merged_wines_df = merged_wines_df.withColumn("Age", substring(merged_wines_df["Age"], 1, 2).cast("int"))

# Definindo as condições para categorizar as idades em grupos
conditions = [
    (merged_wines_df['Age'].between(18, 30)),
    (merged_wines_df['Age'].between(31, 49)),
    (merged_wines_df['Age'] >= 50)
]

# Definindo os rótulos para cada grupo
labels = ['18 - 30', '31 - 49', '50 ou mais']

# Aplicando as condições e atribuindo os rótulos aos grupos em uma nova coluna 'Faixa Etária'
merged_wines_df = merged_wines_df.withColumn('Age Group', 
                                             when(conditions[0], labels[0])
                                            .when(conditions[1], labels[1])
                                            .otherwise(labels[2]))

display(merged_wines_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Filtrando as colunas

# COMMAND ----------

# Selecionando apenas as colunas necessárias
merged_wines_df = merged_wines_df.select('id', "Gender","Age Group", "City","Country","Id_Wine", "Variety","Country_Wine","Province","FavoriteWine" ,"LikeSports", "LikeGames", "LikeNetflix")

# Mostrando o DataFrame resultante
display(merged_wines_df)


# COMMAND ----------

#merged_df = people_df.join(api_df, on="id", how="right")
#display(merged_df)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Salvar DataFrame como Parquet") \
    .getOrCreate()


caminho_para_silver = "dbfs:/mnt/dados/silver/"

# Salvar o DataFrame como arquivo Parquet na pasta "silver"
merged_wines_df.write.parquet(f"{caminho_para_silver}/merged_wines.parquet")


