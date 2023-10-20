# Databricks notebook source
# MAGIC %md 
# MAGIC # Kafka e SparkStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalação de dependências

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install newsapi-python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Função de Pesquisa

# COMMAND ----------

# MAGIC %run ./api_key

# COMMAND ----------

from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key=api_key)

def pesquisa(termo):
    resultado = newsapi.get_everything(q=termo,
                                        language='pt',
                                        sort_by='relevancy'
                                        )
    if resultado['status']!='ok':
        return []
        
    for artigo in resultado['articles']:
        artigo['autor'] = artigo.pop('author')
        artigo['titulo'] = artigo.pop('title')
        artigo['descricao'] = artigo.pop('description')
        artigo['publicado_em'] = artigo.pop('publishedAt')
        # artigo['url'] = artigo['url']
        artigo['conteudo'] = artigo.pop('content')
        artigo['fonte'] = artigo['source']['name']
        artigo['criterio_relevancia']=termo
        del artigo['source']
        del artigo['urlToImage']

    
    return resultado['articles']


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Kafka e Spark Streaming

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql.functions import from_json, col

df = spark.readStream.format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('subscribe', 'termos')\
        .option('failOnDataLoss', 'False')\
        .load()

df_termos = df.selectExpr('CAST(value as STRING)')\
                .select(col('value').alias('termo'))


schema_pesquisa = ArrayType(
    StructType() \
        .add("autor", StringType()) \
        .add("titulo", StringType()) \
        .add("descricao", StringType()) \
        .add("url", StringType()) \
        .add("publicado_em", StringType()) \
        .add("conteudo", StringType()) \
        .add("fonte", StringType()) \
        .add("criterio_relevancia", StringType())
)

pesquisa_udf = udf(pesquisa, schema_pesquisa)
df_termos = df_termos.withColumn('resultado', pesquisa_udf('termo'))
df_artigos = df_termos.selectExpr('explode(resultado) as resultado')\
                .select('resultado.*')


local_streaming = '/FileStore/tables/projeto/artigos_streaming.parquet'
checkpoint = '/FileStore/topico_artigos/_checkpoint'
# df_artigos.writeStream.format('parquet')\
#         .option('path', local_streaming)\
#         .option('checkpointlocation', checkpoint)\
#         .start()\
#         .awaitTermination()
display(df_artigos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Testes

# COMMAND ----------

import pyspark.pandas as ps
df = ps.read_parquet(local_streaming)
df.criterio_relevancia.value_counts()

# COMMAND ----------

