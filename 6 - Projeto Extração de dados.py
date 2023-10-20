# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto - Extração de Dados - Brenda Monteiro, Eric Nicholas Oliveira, Gustavo Guedes, Ramon Alves e Renan Rego

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. INSTALANDO E IMPORTANDO A NEWSAPI

# COMMAND ----------

!pip install --upgrade pip wheel
!pip install newsapi-python

# COMMAND ----------

from newsapi import NewsApiClient
import pyspark.pandas as ps
from datetime import datetime
import json
from time import monotonic

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CHAVE DA API

# COMMAND ----------

# MAGIC %run ./api_key

# COMMAND ----------

newsapi = NewsApiClient(api_key=api_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. EXTRAÇÃO DE DADOS

# COMMAND ----------

def extracao_dados():
    criterios = ['genoma', 'sequenciamento de dna', 'terapias geneticas personalizadas', 'medicina personalizada', 'Fibrose cística', 'Hemofilia']
    artigos = []
    
    for criterio in criterios:
        resultado = newsapi.get_everything(q=criterio,
                                            language='pt',
                                            sort_by='relevancy'
                                            )
        if resultado['status']=='ok':
            
            for artigo in resultado['articles']:
                artigo['criterio_relevancia']=criterio
                artigo['fonte'] = artigo['source']['name']
                del artigo['source']
            artigos.extend(resultado['articles'])

    print(f'Quantidade de artigos achados: {len(artigos)}')
    
    colunas_renomeadas = {
        'author':'autor',
        'title':'titulo',
        'description':'descricao',
        'publishedAt':'publicado_em',
        'content':'conteudo'    
    }
    colunas_selecao = ['titulo','descricao','conteudo','autor','fonte','url','publicado_em','criterio_relevancia']

    df = ps.DataFrame(artigos)
    #df = df.drop_duplicates(subset=['url']).reset_index(drop=True)
    df = df.loc[df.title!='[Removed]']
    df = df.fillna('')
    df = df.rename(columns=colunas_renomeadas)
    df = df[colunas_selecao]
    df['publicado_em'] = ps.to_datetime(df['publicado_em'], format='%Y-%m-%dT%H:%M:%SZ')
    return df # df_extraido

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SALVAMENTO DOS DADOS BRUTOS

# COMMAND ----------


def salvar_dados_brutos(df_extraido, local_arquivo_bruto):
    try: #caso já exista o arquivo transformado, segue direto com a concatenação e com a carga do resultado final
        arquivo = dbutils.fs.ls(local_arquivo_bruto)
        df_final = ps.read_parquet(local_arquivo_bruto)
        df_final = ps.concat([df_final, df_extraido])
        df_final = df_final.drop_duplicates(subset=['URL']) # PARA REMOVER AS POSSÍVEIS NOTÍCIAS DUPLICADAS
        df_final.to_parquet(local_arquivo_bruto, utf8_mode='utf8', index=False)
        

    except Exception as e: #caso o arquivo transformando ainda não exista, quer dizer que é o primeiro processo do pipeline e é preciso criar o arquivo destino
        if 'java.io.FileNotFoundException' in str(e):
            print("Arquivo não encontrado, primeiro processamento")
            df_extraido.to_parquet(local_arquivo_bruto, utf8_mode='utf8', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. TRANSFORMAÇÃO DOS DADOS

# COMMAND ----------

def noticias_ano_mes_dia(local_arquivo_bruto, arquivo_noticias_ano_mes_dia):

    df = ps.read_parquet(local_arquivo_bruto) 
    df = df.groupby(by=[df.publicado_em.dt.date]).agg(quantidade=('titulo', 'count')).sort_index()
    df = df.reset_index(drop=False)

    df.to_parquet(arquivo_noticias_ano_mes_dia, utf8_mode='utf8', index=False)
    print("transformação ano mes e dia realizada com sucesso")


def noticias_fonte_autor(local_arquivo_bruto, arquivo_fonte_autor):

    df = ps.read_parquet(local_arquivo_bruto) 
    df = df.groupby(by=['fonte','autor']).agg(quantidade=('titulo', 'count'))
    #reinicia o index e não exclui o anterior
    df = df.reset_index(drop=False)

    df.to_parquet(arquivo_fonte_autor, utf8_mode='utf8', index=False)

    print("transformação fonte autor realizada com sucesso")

def aparicoes_palavras(local_arquivo_bruto, arquivo_aparicoes_palavras):

    df = ps.read_parquet(local_arquivo_bruto) 
    df = df.groupby(by=[df.publicado_em.dt.date, 'criterio_relevancia']).agg(quantidade=('titulo', 'count')).sort_index()

    #reinicia o index e não exclui o anterior
    df = df.reset_index(drop=False)

    df.to_parquet(arquivo_aparicoes_palavras, utf8_mode='utf8', index=False)

    print("transformação aparicao realizada com sucesso")


# COMMAND ----------

def transformacao_dados(local_arquivo_bruto, arquivo_noticias_ano_mes_dia, arquivo_fonte_autor, arquivo_aparicoes_palavras):
    noticias_ano_mes_dia(local_arquivo_bruto, arquivo_noticias_ano_mes_dia)
    noticias_fonte_autor(local_arquivo_bruto, arquivo_fonte_autor)
    aparicoes_palavras(local_arquivo_bruto, arquivo_aparicoes_palavras)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CRIANDO ELT

# COMMAND ----------

TEMPO_EXTRACAO_E_CARGA = 10
TEMPO_TRANSFORMACAO = 40

# COMMAND ----------

def elt(local_arquivo_bruto, arquivo_noticias_ano_mes_dia, arquivo_fonte_autor, arquivo_aparicoes_palavras):
    ultima_extracao_e_carga = monotonic()-TEMPO_EXTRACAO_E_CARGA*2
    ultima_transformacao = monotonic()
    while True:
        tempo_atual = monotonic()

        diff_extracao_e_carga = tempo_atual-ultima_extracao_e_carga
        diff_transformacao = tempo_atual-ultima_transformacao

        if diff_extracao_e_carga>=TEMPO_EXTRACAO_E_CARGA:
            ultima_extracao_e_carga = monotonic()
            print('EXTRAINDO ARQUIVOS')
            df_extraido = extracao_dados()
            print('CARREGANDO ARQUIVO')
            salvar_dados_brutos(df_extraido, local_arquivo_bruto)

        if diff_transformacao>=TEMPO_TRANSFORMACAO:
            ultima_transformacao = monotonic()
            print('TRANSFORMANDO ARQUIVO')
            transformacao_dados(local_arquivo_bruto, arquivo_noticias_ano_mes_dia, arquivo_fonte_autor, arquivo_aparicoes_palavras)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MAIN

# COMMAND ----------

local_arquivo_bruto = "/FileStore/tables/projeto/projeto_noticias_consolidadas.parquet" # Arquivo Bruto sem transformação
arquivo_noticias_ano_mes_dia = "/FileStore/tables/projeto/projeto_noticias_ano_mes_dia.parquet"
arquivo_fonte_autor = "/FileStore/tables/projeto/projeto_noticias_fonte_autor.parquet"
arquivo_aparicoes_palavras = "/FileStore/tables/projeto/projeto_noticias_aparicoes_palavras_chave.parquet"


elt(local_arquivo_bruto, arquivo_noticias_ano_mes_dia, arquivo_fonte_autor, arquivo_aparicoes_palavras)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. VISUALIZANDO OS ARQUIVOS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1. Arquivo Bruto

# COMMAND ----------

local_arquivo_bruto = "/FileStore/tables/projeto/projeto_noticias_consolidadas.parquet" # Arquivo Bruto sem transformação
arquivo_noticias_ano_mes_dia = "/FileStore/tables/projeto/projeto_noticias_ano_mes_dia.parquet"
arquivo_fonte_autor = "/FileStore/tables/projeto/projeto_noticias_fonte_autor.parquet"
arquivo_aparicoes_palavras = "/FileStore/tables/projeto/projeto_noticias_aparicoes_palavras_chave.parquet"

# COMMAND ----------

df_bruto = ps.read_parquet(local_arquivo_bruto) 

df_bruto.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Transformação por Ano, Mês e Dia da Publicação

# COMMAND ----------

df_ano_mes_dia = ps.read_parquet(arquivo_noticias_ano_mes_dia) 

df_ano_mes_dia.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Transformação por Fonte e Autor

# COMMAND ----------

df_fonte_autor = ps.read_parquet(arquivo_fonte_autor) 

df_fonte_autor.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4. Transformação por Quantidade de Aparições das Palavras Chaves

# COMMAND ----------

df_aparicoes_palavras = ps.read_parquet(arquivo_aparicoes_palavras) 

df_aparicoes_palavras.head(20)