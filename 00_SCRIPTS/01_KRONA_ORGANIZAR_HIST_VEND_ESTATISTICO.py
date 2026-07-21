# %%
# Importando bibliotecas
from functions import *
import pandas as pd
import locale
from pathlib import Path
from datetime import datetime
import duckdb
import gc
import numpy as np
import warnings
import logging
import shutil
import time
from joblib import Parallel, delayed
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_percentage_error
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
import os

logging.basicConfig(level=logging.WARNING, format='%(message)s')

warnings.filterwarnings("ignore")

timer = Temporizador()
timer.iniciar()

locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')  # Para Windows
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)
pd.set_option('display.expand_frame_repr', False)

# Detecta se o script está sendo executado de um .py ou de um notebook
try:
    caminho_base = Path(__file__).resolve().parent
except NameError:
    # __file__ não existe em Jupyter ou ambiente interativo
    caminho_base = Path.cwd()

pasta_input_parquet = caminho_base.parent / '01_INPUT_PIPELINE/01_BD_PARQUET'
arquivo_input_regras_negocio = caminho_base.parent / '01_INPUT_PIPELINE/02_REGRAS_NEGOCIO/KRONA_REGRAS.xlsm'
pasta_staging_parquet = caminho_base.parent / '02_STAGING_PARQUET' # Armazena arquivos parquet com tratamentos, aplicações de regras, depara, etc
pasta_input_painel = caminho_base.parent / '03_INPUT_PAINEL' # Armazena arquivos que serão consumidos no painel de S&OP para os gerentes
pasta_painel = caminho_base.parent / '05_PAINEL'
pasta_validacao_anna = caminho_base.parent / '06_VALIDACAO_ANNA'

# # Eliminar arquivos das pastas de 02_STAGING_PARQUET e 03_INPUT_PAINEL que serão regenerados
# pastas_para_limpar = [
#     pasta_staging_parquet,
#     pasta_input_painel,
# ]

# for pasta in pastas_para_limpar:
#     if pasta.exists() and pasta.is_dir():
#         for item in pasta.iterdir():
#             if item.is_file() or item.is_symlink():
#                 item.unlink()
#             elif item.is_dir():
#                 shutil.rmtree(item)

print("✅ Mapeamento de pastas concluído com sucesso!")

# %%
# Carregar dados arquivo KRONA_REGRAS
caminho_arquivo = arquivo_input_regras_negocio

#-----------------------------------------------------------------------#
#--------------- Carregar produtos eliminar ----------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'PRODUTOS_ELIMINAR'
df_produtos_eliminar = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine', dtype={'COD_PROD': str})
df_produtos_eliminar['COD_PROD'] = df_produtos_eliminar['COD_PROD'].astype(str)
df_produtos_eliminar = df_produtos_eliminar.drop_duplicates(subset=['COD_PROD'])
df_produtos_eliminar = df_produtos_eliminar[df_produtos_eliminar['COD_PROD'].notna()].reset_index(drop=True)

#-----------------------------------------------------------------------#
#---------------Carregar Regionais Gestor ------------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'REGIONAIS_GESTOR'
df_regionais_gestor = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine')
df_regionais_gestor = df_regionais_gestor.drop_duplicates(subset=['REGIONAL', 'REGIONAL_GESTOR'])
df_regionais_gestor = df_regionais_gestor[df_regionais_gestor['REGIONAL'].notna()].reset_index(drop=True)

#-----------------------------------------------------------------------#
#---------------Carregar Regionais Construtora -------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'REGIONAIS_CONSTRUTORA'
df_regionais_construtora = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine')
df_regionais_construtora = df_regionais_construtora.drop_duplicates(subset=['REGIONAL BASE', 'REGIONAL ATUALIZADA'])

#-----------------------------------------------------------------------#
#---------------Carregar Clientes para planejamento de Demanda----------#
#-----------------------------------------------------------------------#
guia_excel = 'CLIENTES_DEMANDA'
df_clientes_plan_demanda = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine', dtype={'Cod_Grupo_Cliente': str})
df_clientes_plan_demanda = df_clientes_plan_demanda.drop_duplicates(subset=['Cod_Grupo_Cliente'])
df_clientes_plan_demanda = df_clientes_plan_demanda[df_clientes_plan_demanda['Cod_Grupo_Cliente'].notna()].reset_index(drop=True)

# Converter a coluna de clientes para set para acelerar o isin
lista_clientes_plan_demanda = set(df_clientes_plan_demanda['Cod_Grupo_Cliente'])

# Unir COD_PROD de df_produtos_lancamento e df_produtos_eliminar, formar uma unica lista de produtos a eliminar, e remover do df_fato_vendas_krona
# produtos_a_eliminar = pd.concat([df_produtos_eliminar[['COD_PROD']], df_produtos_lancamento[['COD_PROD']]]).drop_duplicates().reset_index(drop=True)
# FIXME: Retirei os produtos de lançamento da lista de exclusão conforme solicitação da Anna no WORD
produtos_a_eliminar = df_produtos_eliminar[['COD_PROD']].drop_duplicates().reset_index(drop=True)

#-----------------------------------------------------------------------#
#---------------Carregar DIRECIONA_CLIENTES_REGIONAL--------------------#
#-----------------------------------------------------------------------#
guia_excel = 'DIRECIONA_CLIENTES_REGIONAL'
df_direc_cli_regional = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine', dtype={'COD_GRUPO_CLIENTE': str, 'COD_CLIENTE': str})
df_direc_cli_regional = df_direc_cli_regional[df_direc_cli_regional['COD_CLIENTE'].notna()].reset_index(drop=True)

#-----------------------------------------------------------------------#
#---------------Carregar PERIODO_PREVISAO-------------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'PERIODO_PREVISAO'
df_periodo_previsao = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine')
df_periodo_previsao = df_periodo_previsao[df_periodo_previsao['PERIODO_PROJECAO'].notna()].reset_index(drop=True)
df_periodo_previsao = df_periodo_previsao.drop_duplicates(subset=['PERIODO_PROJECAO'])

# Classificar o df_periodo_previsao em ordem crescente de PERIODO_PROJECAO
df_periodo_previsao = df_periodo_previsao.sort_values(by='PERIODO_PROJECAO').reset_index(drop=True)

print("✅ Importação e tratamento de dados do arquivo KRONA_REGRAS, concluídos com sucesso!")

# %%
# Script para eliminar duplicação de Chv_Cliente no Dim_Clientes_Krona, conforme orientado por Marcos TI, criamos essa rotina para encontrar as duplicações, eliminar e gerar novo Parquet sem duplicações.

# Carregar o Parquet
df_dim_cli_krona = pd.read_parquet(pasta_input_parquet / "Dim_Clientes_Krona.parquet")

# Eliminar duplciações mantendo a primeira ocorrência
df_dim_cli_krona = df_dim_cli_krona.drop_duplicates(subset=["Chv_Cliente"], keep='first').reset_index(drop=True)

# Gerar novo Parquet sem duplicações
df_dim_cli_krona.to_parquet(pasta_input_parquet / "Dim_Clientes_Krona.parquet", index=False)

del df_dim_cli_krona
gc.collect()

# %%
# Criando uma DIM_PRODUTOS_KRONA organizada e resumida, para consumir dados de produtos e principalmente peso unitário

# Carregando DIM_PRODUTOS_VENDAS_KRONA, filtrando Nom_Empresa que contenha "Krona" para eliminar produtos de outras empresas que possam estar na base, e selecionando apenas as colunas necessárias para o planejamento de demanda
dim_produtos = pd.read_parquet(
    pasta_input_parquet / "Dim_Produtos_Vendas_Krona.parquet",
    columns=["Cod_Produto", "Des_Produto", "Num_Peso", "Cod_Familia", "Des_Familia", "Cod_Linha", "Des_Linha", "Nom_Empresa"]
)
dim_produtos = dim_produtos[dim_produtos["Nom_Empresa"].str.contains("Krona")]

# Drop coluna Nom_Empresa, pois já filtramos apenas os produtos da Krona
dim_produtos = dim_produtos.drop(columns=["Nom_Empresa"])

cols_str = ["Cod_Produto", "Cod_Familia", "Cod_Linha"]

dim_produtos[cols_str] = dim_produtos[cols_str].astype("string")

# Eliminar duplicas de Cod_Produto
dim_produtos = dim_produtos.drop_duplicates(subset=["Cod_Produto"], keep='first').reset_index(drop=True)

# Concatenar Cod_Familia com Des_Familia, Cod_Linha com Des_Linha e criar colunas novas para isso, e eliminar as colunas antigas de código e descrição de família e linha
dim_produtos["FAMILIA"] = dim_produtos["Cod_Familia"] + " - " + dim_produtos["Des_Familia"]
dim_produtos["LINHA"] = dim_produtos["Cod_Linha"] + " - "+ dim_produtos["Des_Linha"]
dim_produtos = dim_produtos.drop(columns=["Cod_Familia", "Des_Familia", "Cod_Linha", "Des_Linha"])

# REnomar colunas para manter padrão de nomenclatura
dim_produtos.rename(columns={"Cod_Produto": "COD_PROD", "Des_Produto": "DESC_PROD", "Num_Peso": "PESO_UNIT"}, inplace=True)

# Salvar na pasta staging em formato parquet para uso posterior
dim_produtos.to_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet", index=False)

# %%
# # FIXME apagar depois da Anna validar
# # Filtrado Ano 2024 conforme solicitado por Karolina

# empresa = 'Krona'
# ano_filtro = 2024

# vendas = (pasta_input_parquet / "Fato_Vendas_Krona.parquet").as_posix()
# produtos = (pasta_input_parquet / "Dim_Produtos_Vendas_Krona.parquet").as_posix()
# clientes = (pasta_input_parquet / "Dim_Clientes_Krona.parquet").as_posix()
# vendedores = (pasta_input_parquet / "Dim_Vendedores_Krona.parquet").as_posix()

# con = duckdb.connect()

# con.register(
#     "map_reg",
#     df_regionais_construtora[['REGIONAL BASE', 'REGIONAL ATUALIZADA']]
# )

# con.register(
#     "direc_cli_regional",
#     df_direc_cli_regional[['COD_CLIENTE', 'REGIONAL']]
# )

# con.register(
#     "reg_gestor",
#     df_regionais_gestor[['REGIONAL', 'REGIONAL_GESTOR']]
# )

# sql = f"""
# WITH
# fato AS (
#   SELECT
#     Cod_Produto,
#     Chv_Cliente,
#     Chv_Vendedor,

#     COALESCE(
#       TRY_CAST(Dat_Emissao_Venda AS DATE),
#       CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
#       CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
#     ) AS DATA_EMISSAO,

#     CASE
#       WHEN EXTRACT(
#         DAY FROM COALESCE(
#           TRY_CAST(Dat_Emissao_Venda AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
#         )
#       ) >= 21
#       THEN DATE_TRUNC(
#         'month',
#         COALESCE(
#           TRY_CAST(Dat_Emissao_Venda AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
#         ) + INTERVAL 1 MONTH
#       )
#       ELSE DATE_TRUNC(
#         'month',
#         COALESCE(
#           TRY_CAST(Dat_Emissao_Venda AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
#           CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
#         )
#       )
#     END AS DATA_COTA,

#     TRIM(Nom_Empresa) AS EMPRESA,
#     SUM(TRY_CAST(Qtd_Venda AS DOUBLE)) AS QTD_VENDA,
#     SUM(TRY_CAST(Qtd_Peso_Venda AS DOUBLE)) AS VOL_VENDA
#   FROM parquet_scan('{vendas}')
#   WHERE UPPER(TRIM(Nom_Empresa)) LIKE '%{empresa.strip().upper()}%'
#     AND UPPER(TRIM(Des_Origem)) LIKE '%{empresa.strip().upper()}%'
#     AND Cod_Empresa IN ('01','05','08','0802','10')
#     AND TRY_CAST(NULLIF(TRIM(Cod_Bloqueio), '') AS INTEGER) IN (80,90,95,99,60,81)
#     AND TRY_CAST(Qtd_Venda AS DOUBLE) > 0
#     AND Dat_Emissao_Venda IS NOT NULL
#     AND TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)) <> ''
#     AND COALESCE(
#       TRY_CAST(Dat_Emissao_Venda AS DATE),
#       CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
#       CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
#     ) >= DATE '2022-01-01'
#   GROUP BY
#     Cod_Produto,
#     Chv_Cliente,
#     Chv_Vendedor,
#     DATA_EMISSAO,
#     DATA_COTA,
#     EMPRESA
# ),

# prod AS (
#   SELECT
#     Cod_Produto,
#     TRIM(Des_Produto) AS DESC_PRODUTO,
#     Cod_Familia,
#     TRIM(Des_Familia) AS Des_Familia,
#     Cod_Linha,
#     TRIM(Des_Linha) AS Des_Linha,
#     TRIM(Nom_Empresa) AS EMPRESA,
#     TRY_CAST(Num_Peso AS DOUBLE) AS PESO_UNIT
#   FROM parquet_scan('{produtos}')
#   WHERE Des_Linha IS NOT NULL
#     AND TRIM(Des_Linha) <> ''
#     AND Cod_Empresa IN ('01','05','08','0802','10')
# ),

# cli AS (
#   SELECT
#     Chv_Cliente,
#     TRIM(Nom_Cliente) AS NOME_CLIENTE,
#     TRIM(Nom_Empresa) AS EMPRESA,
#     Chv_Vendedor_Cliente,
#     TRIM(Des_Segmento) AS SEGMENTO,
#     CASE
#       WHEN TRIM(Cod_Grupo_Cliente) = '' OR Cod_Grupo_Cliente IS NULL
#       THEN TRIM(SPLIT_PART(Chv_Cliente, '|', 2))
#       ELSE TRIM(Cod_Grupo_Cliente)
#     END AS COD_GRUPO_CLIENTE,
#     CASE
#       WHEN TRIM(Des_Grupo_e_Cliente) = '' OR Des_Grupo_e_Cliente IS NULL
#       THEN TRIM(Nom_Cliente)
#       ELSE TRIM(Des_Grupo_e_Cliente)
#     END AS DESC_GRUPO_E_CLIENTE
#   FROM parquet_scan('{clientes}')
# ),

# vend AS (
#   SELECT
#     Chv_Vendedor,
#     TRIM(Des_Regiao) AS Des_Regiao
#   FROM parquet_scan('{vendedores}')
# ),

# base_final AS (
#   SELECT
#     f.EMPRESA,
#     TRIM(SPLIT_PART(c.Chv_Cliente, '|', 2)) AS COD_CLIENTE,
#     c.NOME_CLIENTE,
#     c.COD_GRUPO_CLIENTE,
#     c.DESC_GRUPO_E_CLIENTE,
#     c.SEGMENTO,
#     f.Cod_Produto AS COD_PRODUTO,
#     p.DESC_PRODUTO,
#     CAST(p.Cod_Familia AS VARCHAR) || ' - ' || p.Des_Familia AS FAMILIA,
#     CAST(p.Cod_Linha AS VARCHAR) || ' - ' || p.Des_Linha AS LINHA,
#     p.PESO_UNIT,
#     v1.Des_Regiao AS REGIAO_CLIENTE,
#     v2.Des_Regiao AS REGIAO_MOVIMENTO,
#     DATE_TRUNC('month', f.DATA_EMISSAO) AS PERIODO_EMISSAO,
#     DATE_TRUNC('month', f.DATA_COTA) AS PERIODO_COTA,
#     EXTRACT(MONTH FROM f.DATA_EMISSAO) AS MES_EMISSAO,
#     EXTRACT(MONTH FROM f.DATA_COTA) AS MES_COTA,
#     f.QTD_VENDA,
#     f.VOL_VENDA
#   FROM fato f
#   LEFT JOIN prod p
#     ON f.Cod_Produto = p.Cod_Produto
#    AND f.EMPRESA = p.EMPRESA
#   LEFT JOIN cli c
#     ON f.Chv_Cliente = c.Chv_Cliente
#    AND f.EMPRESA = c.EMPRESA
#   LEFT JOIN vend v1
#     ON c.Chv_Vendedor_Cliente = v1.Chv_Vendedor
#   LEFT JOIN vend v2
#     ON f.Chv_Vendedor = v2.Chv_Vendedor
# ),

# regional_tratada AS (
#   SELECT
#     b.*,
#     COALESCE(NULLIF(b.REGIAO_CLIENTE, ''), b.REGIAO_MOVIMENTO) AS RC_FIX,
#     UPPER(COALESCE(b.SEGMENTO, '')) AS SEG_UP,
#     UPPER(COALESCE(b.REGIAO_CLIENTE, '')) AS RC,
#     UPPER(COALESCE(b.REGIAO_MOVIMENTO, '')) AS RM,

#     CASE
#       WHEN d.REGIONAL IS NOT NULL AND TRIM(d.REGIONAL) <> '' THEN d.REGIONAL
#       WHEN UPPER(COALESCE(b.SEGMENTO, '')) LIKE '%CONSTRUTORA%'
#         OR UPPER(COALESCE(b.SEGMENTO, '')) LIKE '%INSTALADOR%'
#         THEN COALESCE(m."REGIONAL ATUALIZADA", COALESCE(NULLIF(b.REGIAO_CLIENTE, ''), b.REGIAO_MOVIMENTO))
#       WHEN UPPER(COALESCE(b.REGIAO_CLIENTE, '')) = 'TELEVENDAS'
#        AND UPPER(COALESCE(b.REGIAO_MOVIMENTO, '')) = 'TELEVENDAS'
#         THEN 'TELEVENDAS'
#       WHEN UPPER(COALESCE(b.REGIAO_CLIENTE, '')) <> 'TELEVENDAS'
#        AND UPPER(COALESCE(b.REGIAO_MOVIMENTO, '')) = 'TELEVENDAS'
#         THEN 'TELEVENDAS'
#       WHEN UPPER(COALESCE(b.REGIAO_CLIENTE, '')) = 'TELEVENDAS'
#        AND UPPER(COALESCE(b.REGIAO_MOVIMENTO, '')) <> 'TELEVENDAS'
#         THEN b.REGIAO_MOVIMENTO
#       ELSE COALESCE(NULLIF(b.REGIAO_CLIENTE, ''), b.REGIAO_MOVIMENTO)
#     END AS REGIONAL
#   FROM base_final b
#   LEFT JOIN map_reg m
#     ON m."REGIONAL BASE" = b.REGIAO_CLIENTE
#   LEFT JOIN direc_cli_regional d
#     ON d.COD_CLIENTE = b.COD_CLIENTE
# ),

# final AS (
#   SELECT
#     g.REGIONAL_GESTOR,
#     r.REGIONAL,
#     r.REGIAO_CLIENTE,
#     r.REGIAO_MOVIMENTO,
#     r.COD_PRODUTO,
#     r.DESC_PRODUTO,
#     r.FAMILIA,
#     r.LINHA,
#     r.PERIODO_EMISSAO,
#     r.PERIODO_COTA,
#     r.MES_EMISSAO,
#     r.MES_COTA,
#     SUM(r.QTD_VENDA) AS QTD_VENDA,
#     SUM(r.VOL_VENDA) AS VOL_VENDA
#   FROM regional_tratada r
#   LEFT JOIN reg_gestor g
#     ON r.REGIONAL = g.REGIONAL
#   WHERE EXTRACT(YEAR FROM r.PERIODO_COTA) = {ano_filtro}
#   GROUP BY
#     g.REGIONAL_GESTOR,
#     r.REGIONAL,
#     r.REGIAO_CLIENTE,
#     r.REGIAO_MOVIMENTO,
#     r.COD_PRODUTO,
#     r.DESC_PRODUTO,
#     r.FAMILIA,
#     r.LINHA,
#     r.PERIODO_EMISSAO,
#     r.PERIODO_COTA,
#     r.MES_EMISSAO,
#     r.MES_COTA
# )

# SELECT *
# FROM final
# """

# df_validacao_final_data_emissao_cota = con.execute(sql).df()

# df_validacao_final_data_emissao_cota.to_parquet(
#     pasta_validacao_anna / "VALIDACAO_FINAL_DATA_EMISSAO_COTA.parquet",
#     index=False
# )

# del df_validacao_final_data_emissao_cota
# gc.collect()

# print("✅ Carregamento de VALIDACAO_FINAL_DATA_EMISSAO_COTA concluído com sucesso!")

# %%
# # FIXME teste para Anna

# output = (pasta_validacao_anna / "VALIDACAO_FINAL_DATA_EMISSAO_COTA.parquet").as_posix()
# df = pd.read_parquet(output)

# # Salvar em excel
# excel_output = (pasta_validacao_anna / "VALIDACAO_FINAL_DATA_EMISSAO_COTA.xlsx").as_posix()
# df.to_excel(excel_output, index=False)

# %%
empresa = 'Krona'
vendas = (pasta_input_parquet / "Fato_Vendas_Krona.parquet").as_posix()
produtos = (pasta_input_parquet / "Dim_Produtos_Vendas_Krona.parquet").as_posix()
clientes  = (pasta_input_parquet / "Dim_Clientes_Krona.parquet").as_posix()
vendedores = (pasta_input_parquet / "Dim_Vendedores_Krona.parquet").as_posix()

sql = f"""
WITH
fato AS (
  SELECT
    Cod_Produto,
    Chv_Cliente,
    Chv_Vendedor,
    CASE
      WHEN EXTRACT(
        DAY FROM COALESCE(
          TRY_CAST(Dat_Emissao_Venda AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
        )
      ) >= 21
      THEN DATE_TRUNC(
        'month',
        COALESCE(
          TRY_CAST(Dat_Emissao_Venda AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
        ) + INTERVAL 1 MONTH
      )
      ELSE DATE_TRUNC(
        'month',
        COALESCE(
          TRY_CAST(Dat_Emissao_Venda AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
          CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
        )
      )
    END AS PERIODO,
    TRIM(Nom_Empresa) AS EMPRESA,
    SUM(TRY_CAST(Qtd_Venda AS DOUBLE)) AS QTD_VENDA,
    SUM(TRY_CAST(Qtd_Peso_Venda AS DOUBLE)) AS VOL_VENDA
  FROM parquet_scan('{vendas}')
  WHERE UPPER(TRIM(Nom_Empresa)) LIKE '%{empresa.strip().upper()}%'
    AND UPPER(TRIM(Des_Origem))  LIKE '%{empresa.strip().upper()}%'
    AND Cod_Empresa IN ('01','05','08','0802','10')
    AND TRY_CAST(NULLIF(TRIM(Cod_Bloqueio), '') AS INTEGER) IN (80,90,95,99,60,81)
    AND TRY_CAST(Qtd_Venda AS DOUBLE) > 0
    AND Dat_Emissao_Venda IS NOT NULL
    AND TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)) <> ''
    AND COALESCE(
      TRY_CAST(Dat_Emissao_Venda AS DATE),
      CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%Y-%m-%d') AS DATE),
      CAST(TRY_STRPTIME(TRIM(CAST(Dat_Emissao_Venda AS VARCHAR)), '%d/%m/%Y') AS DATE)
    ) >= DATE '2022-01-01'
  GROUP BY Cod_Produto, Chv_Cliente, Chv_Vendedor, PERIODO, EMPRESA
),
prod AS (
  SELECT
    Cod_Produto,
    TRIM(Des_Produto) AS Des_Produto,
    Cod_Familia,
    TRIM(Des_Familia) AS Des_Familia,
    Cod_Linha,
    TRIM(Des_Linha) AS Des_Linha,
    TRIM(Nom_Empresa) AS EMPRESA,
    TRY_CAST(Num_Peso AS DOUBLE) AS PESO_UNIT
  FROM parquet_scan('{produtos}')
  WHERE Des_Linha IS NOT NULL
    AND TRIM(Des_Linha) <> ''
    AND Cod_Empresa IN ('01','05','08','0802','10')
),
cli AS (
  SELECT
    Chv_Cliente,
    TRIM(Nom_Cliente) AS NOME_CLIENTE,
    TRIM(Nom_Empresa) AS EMPRESA,
    Chv_Vendedor_Cliente,
    TRIM(Des_Segmento) AS SEGMENTO,
    CASE
      WHEN TRIM(Cod_Grupo_Cliente) = '' OR Cod_Grupo_Cliente IS NULL
      THEN TRIM(SPLIT_PART(Chv_Cliente, '|', 2))
      ELSE TRIM(Cod_Grupo_Cliente)
    END AS COD_GRUPO_CLIENTE,
    CASE
      WHEN TRIM(Des_Grupo_e_Cliente) = '' OR Des_Grupo_e_Cliente IS NULL
      THEN TRIM(Nom_Cliente)
      ELSE TRIM(Des_Grupo_e_Cliente)
    END AS DESC_GRUPO_E_CLIENTE
  FROM parquet_scan('{clientes}')
),
vend AS (
  SELECT
    Chv_Vendedor,
    TRIM(Des_Regiao) AS Des_Regiao
  FROM parquet_scan('{vendedores}')
),
final AS (
  SELECT
    f.EMPRESA,
    TRIM(SPLIT_PART(c.Chv_Cliente, '|', 2)) AS COD_CLIENTE,
    c.NOME_CLIENTE,
    c.COD_GRUPO_CLIENTE,
    c.DESC_GRUPO_E_CLIENTE,
    c.SEGMENTO,
    f.Cod_Produto AS COD_PROD,
    p.Des_Produto AS DESC_PRODUTO,
    CAST(p.Cod_Familia AS VARCHAR) || ' - ' || p.Des_Familia AS FAMILIA,
    CAST(p.Cod_Linha   AS VARCHAR) || ' - ' || p.Des_Linha   AS LINHA,
    v1.Des_Regiao AS REGIAO_CLIENTE,
    v2.Des_Regiao AS REGIAO_MOVIMENTO,
    f.PERIODO,
    f.QTD_VENDA,
    f.VOL_VENDA,
    f.VOL_VENDA / f.QTD_VENDA AS PESO_UNIT
  FROM fato f
  LEFT JOIN prod p ON f.Cod_Produto = p.Cod_Produto AND f.EMPRESA = p.EMPRESA
  LEFT JOIN cli  c ON f.Chv_Cliente = c.Chv_Cliente AND f.EMPRESA = c.EMPRESA
  LEFT JOIN vend v1 ON c.Chv_Vendedor_Cliente = v1.Chv_Vendedor
  LEFT JOIN vend v2 ON f.Chv_Vendedor         = v2.Chv_Vendedor
)
SELECT
  UPPER(EMPRESA) AS EMPRESA,
  COD_CLIENTE,
  NOME_CLIENTE,
  COD_GRUPO_CLIENTE,
  DESC_GRUPO_E_CLIENTE,
  SEGMENTO,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
  PESO_UNIT,
  REGIAO_CLIENTE,
  REGIAO_MOVIMENTO,
  PERIODO,
  QTD_VENDA,
  VOL_VENDA
FROM final
"""
df_vendas_krona_silver = duckdb.query(sql).to_df()

# Salvar em parquet
df_vendas_krona_silver.to_parquet(pasta_staging_parquet / "df_vendas_krona_silver.parquet", index=False)

del df_vendas_krona_silver
gc.collect()

print("✅ Carregamento de df_vendas_krona_silver concluído com sucesso!")

# %%
# ============================================================
# 1. Criando coluna REGIONAL copiando a coluna REGIAO_CLIENTE 
#    no df_vendas_krona. 
#    Onde o segmento contém CONSTRUTORA ou INSTALADOR, buscar 
#    na tabela de regionais_construtora a regional atualizada.
# ============================================================

# Carregar o df_vendas_krona_silver do parquet
df_vendas_krona_gold = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona_silver.parquet")

# Cria a tabela de de-para das regionais (já registrada no engine)
duckdb.register("vendas", df_vendas_krona_gold)
duckdb.register("map_reg", df_regionais_construtora[['REGIONAL BASE','REGIONAL ATUALIZADA']])
duckdb.register("direc_cli_regional", df_direc_cli_regional[['COD_CLIENTE', 'REGIONAL']])

sql = """
WITH base AS (
  SELECT
    v.*,
    -- Substitui valores vazios de REGIAO_CLIENTE por REGIAO_MOVIMENTO
    COALESCE(NULLIF(v.REGIAO_CLIENTE,''), v.REGIAO_MOVIMENTO) AS RC_FIX,
    UPPER(v.SEGMENTO) AS SEG_UP,
    UPPER(v.REGIAO_CLIENTE) AS RC,
    UPPER(v.REGIAO_MOVIMENTO) AS RM
  FROM vendas v
),

ajuste AS (
  SELECT
    b.*,
    CASE
      -- >>> ADICIONADO: override por cliente (se existir na df_direc_cli_regional)
      WHEN d.REGIONAL IS NOT NULL AND d.REGIONAL <> '' THEN d.REGIONAL
      
      -- 1) Se SEGMENTO contém CONSTRUTORA ou INSTALADOR => usa de-para
      WHEN b.SEG_UP LIKE '%CONSTRUTORA%' OR b.SEG_UP LIKE '%INSTALADOR%'
        THEN COALESCE(m."REGIONAL ATUALIZADA", b.RC_FIX)
      -- ============================================================
      -- 2. Converter TELEVENDAS - Regras para definir REGIONAL:
      --    REGIONAL = CONSTRUTORA => REGIONAL_CONSTRUTORA
      --    REGIAO_CLIENTE = TELEVENDAS e REGIAO_MOVIMENTO = TELEVENDAS => TELEVENDAS
      --    REGIAO_CLIENTE != TELEVENDAS e REGIAO_MOVIMENTO = TELEVENDAS => TELEVENDAS
      --    REGIAO_CLIENTE = TELEVENDAS e REGIAO_MOVIMENTO != TELEVENDAS => REGIAO_MOVIMENTO
      --    Caso contrário => REGIAO_CLIENTE
      -- ============================================================
      WHEN b.RC='TELEVENDAS' AND b.RM='TELEVENDAS' THEN 'TELEVENDAS'
      WHEN b.RC<>'TELEVENDAS' AND b.RM='TELEVENDAS' THEN 'TELEVENDAS'
      WHEN b.RC='TELEVENDAS' AND b.RM<>'TELEVENDAS' THEN b.RM
      ELSE b.RC_FIX
    END AS REGIONAL
  FROM base b
  LEFT JOIN map_reg m
    ON m."REGIONAL BASE" = b.REGIAO_CLIENTE
  -- >>> ADICIONADO: join com regional direcionada por clientez
  LEFT JOIN direc_cli_regional d
    ON d.COD_CLIENTE = b.COD_CLIENTE
)

-- ============================================================
-- Resultado final consolidado
-- ============================================================
SELECT
  EMPRESA,
  COD_CLIENTE,
  NOME_CLIENTE,
  COD_GRUPO_CLIENTE,
  DESC_GRUPO_E_CLIENTE,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
  REGIONAL,
  REGIAO_CLIENTE,   -- ADICIONADO
  REGIAO_MOVIMENTO, -- ADICIONADO
  PERIODO,
  SUM(QTD_VENDA) AS QTD_VENDA,
  SUM(VOL_VENDA) AS VOL_VENDA
FROM ajuste
-- WHERE REGIONAL IS NOT NULL AND REGIONAL <> ''
GROUP BY
  EMPRESA,
  COD_CLIENTE,
  NOME_CLIENTE,
  COD_GRUPO_CLIENTE,
  DESC_GRUPO_E_CLIENTE,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
  REGIAO_MOVIMENTO, -- ADICIONADO
  REGIAO_CLIENTE,   -- ADICIONADO
  REGIONAL,
  PERIODO
"""

# Executa no DuckDB
df_vendas_krona_gold= duckdb.query(sql).to_df()

# Inserir REGIONAL_GESTOR no df_vendas_krona
df_vendas_krona_gold = pd.merge(
    df_vendas_krona_gold,
    df_regionais_gestor,
    left_on='REGIONAL',
    right_on='REGIONAL',
    how='left'
)

colunas_ordenadas = [
    "EMPRESA",
    "COD_CLIENTE",
    "NOME_CLIENTE",
    "COD_GRUPO_CLIENTE",
    "DESC_GRUPO_E_CLIENTE",
    "COD_PROD",
    "DESC_PRODUTO",
    "FAMILIA",
    "LINHA",
    "REGIONAL",
    "REGIAO_CLIENTE",   # ADICIONADO
    "REGIAO_MOVIMENTO", # ADICIONADO
    "REGIONAL_GESTOR",
    "PERIODO",
    "QTD_VENDA",
    "VOL_VENDA"
]

df_vendas_krona_gold = df_vendas_krona_gold[colunas_ordenadas]

# Salvar df_vendas_krona_gold em Parquet para salvar as alterações, filtros e regras aplicadas no histórico, otimizando memória e garantindo rastreabilidade
df_vendas_krona_gold.to_parquet(pasta_staging_parquet / "df_vendas_krona_gold.parquet", index=False)

del df_vendas_krona_gold
gc.collect()

print("✅ Organização de Regionais e Inserção de Regional Gestor na df_vendas_krona_gold concluídos com sucesso!")

# %%
# # FIXME: Gerar arquivo de saída para validação Anna
# df_vendas_krona_gold = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona_gold.parquet")

# # Filtrar somente PERIODO = 2025-01-01 a 2025-12-31
# df_vendas_krona_gold = df_vendas_krona_gold[
#     (df_vendas_krona_gold['PERIODO'] >= '2025-01-01') &
#     (df_vendas_krona_gold['PERIODO'] <= '2025-12-31')
# ].reset_index(drop=True)

# cols_agreg = [
#     'REGIONAL_GESTOR', 'REGIONAL', 'REGIAO_CLIENTE', 'REGIAO_MOVIMENTO', 'COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'PERIODO'
# ]

# df_vendas_krona_gold[cols_agreg] = (
#     df_vendas_krona_gold[cols_agreg]
#     .astype('string')
#     .apply(lambda c: c.str.strip())
#     .fillna('UNKNOWN')
#     .replace('', 'UNKNOWN')
# )

# # Agrupando dados por REGIONAL, COD_PROD, DESC_PRODUTO, FAMILIA, LINHA e PERIODO
# df_validacao_anna = (
#     df_vendas_krona_gold
#     .groupby(cols_agreg, as_index=False)
#     .agg(QTD_VENDA=('QTD_VENDA', 'sum'))
# )

# # Gerar excel para validação Anna
# df_validacao_anna.to_excel(pasta_validacao_anna / "VALIDACAO_ANNA_HIST_VEND_DATA_COTA_KRONA.xlsx", index=False)

# del df_vendas_krona_gold, df_validacao_anna
# gc.collect()

# %%
# Aplicar produtos a eliminar no df_vendas_krona_gold, e excluir os produtos listados na variavel produtos_a_eliminar vinda do arquivo de regras de negócio
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona_gold.parquet")
lista_produtos_eliminar = set(produtos_a_eliminar['COD_PROD'])
df_vendas_krona = df_vendas_krona[~df_vendas_krona['COD_PROD'].isin(lista_produtos_eliminar)]
df_vendas_krona.to_parquet(pasta_staging_parquet / "df_vendas_krona.parquet", index=False)

del df_vendas_krona
gc.collect()

print("✅ Eliminação de produtos concluída!")

# %%

# Criar demanda de lançamento, conforme regras alinhadas com a Anna

#-----------------------------------------------------------------------#
#---------------Carrregar Demanda Lançamento Novos Produtos ------------#
#-----------------------------------------------------------------------#
caminho_arquivo = arquivo_input_regras_negocio

guia_excel = 'PRODUTOS_LANCAMENTOS'
df_demanda_produtos_lancamento = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine', dtype={'COD': str})

# Colunas em Maiuscula
colunas_info_lancamento = [
    'NOME',
    'NOME PROJETO',
    'MARCA',
    'FAMÍLIA',
    'PROCESSO'
]

for col in colunas_info_lancamento:
    df_demanda_produtos_lancamento[col] = (
        df_demanda_produtos_lancamento[col]
        .astype('string')
        .str.strip()
        .str.upper()
    )

if df_demanda_produtos_lancamento.empty:
    raise ValueError(
        "❌ ERRO: Nenhuma informação foi encontrada na aba PRODUTOS_LANCAMENTOS.\n"
    )

df_demanda_produtos_lancamento['JANELA LANÇAMENTO'] = df_demanda_produtos_lancamento['JANELA LANÇAMENTO'].astype(str).str.strip()

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento[df_demanda_produtos_lancamento['JANELA LANÇAMENTO'] != ''].reset_index(drop=True)

df_demanda_produtos_lancamento.rename(columns={'COD': 'COD_PROD'}, inplace=True)

df_demanda_produtos_lancamento['COD_PROD'] = (
    df_demanda_produtos_lancamento['COD_PROD']
    .astype(str)
    .str.strip()
    .str.zfill(4)
)

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento[df_demanda_produtos_lancamento['COD_PROD'].notna()].reset_index(drop=True)
df_demanda_produtos_lancamento['COD_PROD'] = df_demanda_produtos_lancamento['COD_PROD'].astype(str)

# Identifica colunas de data
col_datas = []
for col in df_demanda_produtos_lancamento.columns:
    try:
        pd.to_datetime(col, dayfirst=True, errors='raise')
        col_datas.append(col)
    except:
        continue

colunas_info_lancamento = [
    'NOME',
    'NOME PROJETO',
    'MARCA',
    'FAMÍLIA',
    'PROCESSO'
]

colunas_validas = (
    ['COD_PROD']
    + colunas_info_lancamento
    + [col for col in df_demanda_produtos_lancamento.columns if 'CD:' in str(col)]
    + col_datas
)

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento[colunas_validas]

# Melt
df_demanda_produtos_lancamento = df_demanda_produtos_lancamento.melt(
    id_vars=[col for col in df_demanda_produtos_lancamento.columns if col not in col_datas],
    value_vars=col_datas,
    var_name='PERIODO',
    value_name='VALOR'
)

df_demanda_produtos_lancamento['PERIODO'] = pd.to_datetime(df_demanda_produtos_lancamento['PERIODO']).dt.normalize()


#-----------------------------------------------------------------------#
#---------------Carregar PERIODO_PREVISAO-------------------------------#
#-----------------------------------------------------------------------#
df_periodo_previsao = pd.read_excel(caminho_arquivo, sheet_name='PERIODO_PREVISAO', engine='calamine')
df_periodo_previsao = df_periodo_previsao[df_periodo_previsao['PERIODO_PROJECAO'].notna()]
df_periodo_previsao['PERIODO_PROJECAO'] = pd.to_datetime(df_periodo_previsao['PERIODO_PROJECAO'])

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento[
    df_demanda_produtos_lancamento['PERIODO'].isin(df_periodo_previsao['PERIODO_PROJECAO'])
].reset_index(drop=True)

#-----------------------------------------------------------------------#
# FASE 1 - REPLICAÇÃO (VALOR ANTERIOR NO TEMPO)
#-----------------------------------------------------------------------#

df_demanda_produtos_lancamento['VALOR'] = pd.to_numeric(df_demanda_produtos_lancamento['VALOR'], errors='coerce')

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento.sort_values(['COD_PROD', 'PERIODO'])

# 0 vira NaN
df_demanda_produtos_lancamento['VALOR'] = df_demanda_produtos_lancamento['VALOR'].replace(0, np.nan)

# Forward fill por produto
df_demanda_produtos_lancamento['VALOR'] = (
    df_demanda_produtos_lancamento
    .groupby('COD_PROD')['VALOR']
    .ffill()
)

#-----------------------------------------------------------------------#
# FASE 2 - MÉDIA 3M (FALLBACK)
#-----------------------------------------------------------------------#
# Produtos que ainda não têm nenhum valor
produtos_sem_valor = (
    df_demanda_produtos_lancamento
    .groupby('COD_PROD')['VALOR']
    .max()
)

produtos_sem_valor = produtos_sem_valor[produtos_sem_valor.isna()].index

# Carregar vendas
if 'df_vendas_krona_lancamento' not in locals() or df_vendas_krona_lancamento.empty:
    df_vendas_krona_lancamento = pd.read_parquet(pasta_staging_parquet / 'df_vendas_krona.parquet')

df_vendas_krona_lancamento['PERIODO'] = pd.to_datetime(df_vendas_krona_lancamento['PERIODO'])

data_max = df_vendas_krona_lancamento['PERIODO'].max()
limite = (data_max.to_period('M') - 2).to_timestamp()

df_vendas_krona_3m = df_vendas_krona_lancamento[df_vendas_krona_lancamento['PERIODO'] >= limite]

df_media_3m = (
    df_vendas_krona_3m
    .assign(MES=lambda df: df['PERIODO'].dt.to_period('M'))
    .groupby(['COD_PROD', 'MES'])['QTD_VENDA']
    .sum()
    .groupby('COD_PROD')
    .mean()
    .reset_index()
    .rename(columns={'QTD_VENDA': 'MEDIA_3M'})
)

df_demanda_produtos_lancamento = df_demanda_produtos_lancamento.merge(df_media_3m, on='COD_PROD', how='left')

mask_media = df_demanda_produtos_lancamento['COD_PROD'].isin(produtos_sem_valor)

df_demanda_produtos_lancamento.loc[mask_media, 'VALOR'] = df_demanda_produtos_lancamento.loc[mask_media, 'MEDIA_3M']

df_demanda_produtos_lancamento.drop(columns='MEDIA_3M', inplace=True)

# Final
df_demanda_produtos_lancamento['VALOR'] = df_demanda_produtos_lancamento['VALOR'].fillna(0)

# Carregar DIM_PRODUTOS_KRONA para trazer peso unitário e calcular volume da demanda de lançamento
df_dim_produtos_krona = pd.read_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet")

# Unir df_demanda_produtos_lancamento com df_dim_produtos_krona para trazer peso unitário
df_demanda_produtos_lancamento = df_demanda_produtos_lancamento.merge(df_dim_produtos_krona[["COD_PROD", "PESO_UNIT",'FAMILIA', 'LINHA']], on="COD_PROD", how="left", suffixes=('', '_SOP'))

# PESO_UNIT é preenchido com 0 para os produtos que não encontraram correspondência na DIM_PRODUTOS_KRONA
df_demanda_produtos_lancamento['PESO_UNIT'] = df_demanda_produtos_lancamento['PESO_UNIT'].fillna(0)

# Multiplicar VALOR pela PESO_UNIT para calcular o volume da demanda de lançamento
df_demanda_produtos_lancamento['VOL_LANC'] = df_demanda_produtos_lancamento['VALOR'] * df_demanda_produtos_lancamento['PESO_UNIT']

# Renomear colunas
df_demanda_produtos_lancamento.rename(columns={
    'COD': 'COD_PROD',
    'NOME': 'DESC_PROD',
    'FAMÍLIA': 'FAMILIA',
    'FAMILIA': 'FAMILIA_SOP',
    'VALOR': 'QTD_LANC'
}, inplace=True)

# Reordenar colunas para manter padrão e facilitar análises futuras
colunas_ordenadas = ['COD_PROD', 'DESC_PROD', 'FAMILIA', 'FAMILIA_SOP', 'LINHA', 'MARCA', 'PROCESSO', 'CD: MT', 'CD: NE', 'CD: CO', 'CD: VQ', 'CD: TM', 'PERIODO', 'QTD_LANC', 'VOL_LANC']
df_demanda_produtos_lancamento = df_demanda_produtos_lancamento[colunas_ordenadas]

# Salvar df_demanda_produtos_lancamento ajustado em Parquet
df_demanda_produtos_lancamento.to_parquet(pasta_staging_parquet / "df_demanda_produtos_lancamento.parquet", index=False)

del df_vendas_krona_lancamento, df_dim_produtos_krona
gc.collect()

print("✅ Demanda de lançamento ajustada e concluída!")

# %%
# Retirar do histórico df_vendas_krona os produtos de lançamento, ajustar a demanda lançamento utilizando esse histórico, e gerar um parquet pronto com a demanda de lançamento ajustada para consumo no painel e análises futuras

# Carregando o df_vendas_krona
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")

# Carregar o df_demanda_produtos_lancamento ajustada
df_demanda_produtos_lancamento = pd.read_parquet(pasta_staging_parquet / "df_demanda_produtos_lancamento.parquet")

# Separar o arquivo de vendas retirando os produtos de lançamento para aplicar as regras de lançamento, e depois unir novamente para aplicar a regra de eliminação de produtos
lista_produtos_lancamento = set(df_demanda_produtos_lancamento['COD_PROD'])
df_vendas_krona_lancamento = df_vendas_krona[df_vendas_krona['COD_PROD'].isin(lista_produtos_lancamento)]
df_vendas_krona = df_vendas_krona[~df_vendas_krona['COD_PROD'].isin(lista_produtos_lancamento)]

# Salvar df_vendas_krona sem os produtos de lançamento para aplicar as regras de lançamento
df_vendas_krona.to_parquet(pasta_staging_parquet / "df_vendas_krona.parquet", index=False)

# Salvar df_vendas_krona_lancamento para aplicar as regras de lançamento
df_vendas_krona_lancamento.to_parquet(pasta_staging_parquet / "df_vendas_krona_lancamento.parquet", index=False)

del df_vendas_krona, df_vendas_krona_lancamento, df_demanda_produtos_lancamento
gc.collect()

print("✅ Separação de históricos de produtos de lançamento concluída!")

# %%
# 🦆 Exportação de Dados Vendas para Planejamento Colaborativo
# 🎯 Objetivo: Exportar CSV para o Plano Colaborativo

# Carregando o df_vendas_krona
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")

df_vendas_krona['NIVEL_PLAN_DEMANDA'] = np.where(
    df_vendas_krona['COD_GRUPO_CLIENTE'].isin(lista_clientes_plan_demanda),
    'CLIENTE',
    'PRODUTO'
)

# Separa os DataFrames
df_hist_vend_PRODUTO = df_vendas_krona[df_vendas_krona['NIVEL_PLAN_DEMANDA'] == 'PRODUTO']
df_hist_vend_CLIENTE = df_vendas_krona[df_vendas_krona['NIVEL_PLAN_DEMANDA'] == 'CLIENTE']

# Eliminar coluna NIVEL_PLAN_DEMANDA
df_hist_vend_PRODUTO = df_hist_vend_PRODUTO.drop(columns=['NIVEL_PLAN_DEMANDA'])
df_hist_vend_CLIENTE = df_hist_vend_CLIENTE.drop(columns=['NIVEL_PLAN_DEMANDA'])

# Agrupar df_hist_vend_PRODUTO por REGIONAL_GESTOR, FAMILIA, PERIODO, VOL_VENDA
df_hist_vend_PRODUTO = df_hist_vend_PRODUTO.groupby(
    ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO'],
    as_index=False
).agg({'VOL_VENDA': 'sum'}).reset_index(drop=True)

# Salva como CSV
df_hist_vend_PRODUTO.to_csv(
    pasta_input_painel / 'HIST_VENDA_KRONA_AGREGADO.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

# Agrupar df_hist_vend_CLIENTE por COD_GRUPO_CLIENTE, DESC_GRUPO_E_CLIENTE, REGIONAL_GESTOR, FAMILIA, PERIODO, VOL_VENDA
df_hist_vend_CLIENTE = df_hist_vend_CLIENTE.groupby(
    ["COD_GRUPO_CLIENTE","DESC_GRUPO_E_CLIENTE", "REGIONAL_GESTOR", 'REGIONAL', "FAMILIA", "PERIODO"],
    as_index=False
).agg({'VOL_VENDA': 'sum'}).reset_index(drop=True)

# Salva como CSV
df_hist_vend_CLIENTE.to_csv(
    pasta_input_painel / 'HIST_VENDA_KRONA_CLIENTE.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

# %%
# Gerar os arquivos com média de vendas para Planejamento Colaborativo Agregado
# Encontrar o primeiro dia do mês atual

colunas_agregadas = ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA']

hoje = datetime.today()
primeiro_dia_mes_atual = datetime(hoje.year, hoje.month, 1)

# Calcular o primeiro dia do mês de 6 meses atrás (excluindo mês atual)
primeiro_dia_6_meses_atras = (primeiro_dia_mes_atual - pd.DateOffset(months=6)).to_pydatetime()

# Filtrar apenas os últimos 6 meses (excluindo mês atual)
mask = (df_hist_vend_PRODUTO['PERIODO'] >= primeiro_dia_6_meses_atras) & (df_hist_vend_PRODUTO['PERIODO'] < primeiro_dia_mes_atual)
df_hist_vend_PRODUTO_ultimos_6_meses = df_hist_vend_PRODUTO.loc[mask].copy()

# Ordenar por data crescente
df_hist_vend_PRODUTO_ultimos_6_meses = df_hist_vend_PRODUTO_ultimos_6_meses.sort_values('PERIODO').reset_index(drop=True)

# DataFrame dos 3 meses mais recentes (últimos 3 meses do intervalo filtrado)
df_3_meses_mais_recentes = df_hist_vend_PRODUTO_ultimos_6_meses.copy()

# Identificar as 3 datas mais recentes (sem duplicar por linha)
meses_recentes = sorted(df_3_meses_mais_recentes['PERIODO'].unique())[-3:]

# Filtrar todas as linhas que pertencem a esses 3 meses
df_3_meses_mais_recentes = df_3_meses_mais_recentes[df_3_meses_mais_recentes['PERIODO'].isin(meses_recentes)].copy()

# Agrupa pelas colunas desejadas e calcula a média das colunas numéricas
df_3_meses_mais_recentes_media = df_3_meses_mais_recentes.groupby(colunas_agregadas).mean(numeric_only=True).reset_index()

# Adicionar coluna MEDIA informando 'MÉDIA 3 MESES' na coluna
df_3_meses_mais_recentes_media['MEDIA'] = 'MÉDIA 3 MESES'

# Agrupamento fazendo média dos 6 meses
df_6_meses_mais_recentes_media = df_hist_vend_PRODUTO_ultimos_6_meses.copy()
df_6_meses_mais_recentes_media = df_6_meses_mais_recentes_media.groupby(colunas_agregadas).mean(numeric_only=True).reset_index()

# Adicionar coluna MEDIA informando 'MÉDIA 6 MESES' na coluna
df_6_meses_mais_recentes_media['MEDIA'] = 'MÉDIA 6 MESES'

# Concatenar os DataFrames
df_media_vendas_PRODUTO = pd.concat([df_3_meses_mais_recentes_media, df_6_meses_mais_recentes_media], ignore_index=True)

# Pivotar a coluna MEDIA
df_media_vendas_PRODUTO = df_media_vendas_PRODUTO.pivot_table(
    index=colunas_agregadas,
    columns='MEDIA',
    values='VOL_VENDA',
    aggfunc='sum',
    fill_value=0
).reset_index()

# Gerar o arquivo CSV
df_media_vendas_PRODUTO.to_csv(
    pasta_input_painel / 'MEDIA_VENDA_KRONA_AGREGADO.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

# %%

# Gerar os arquivos com média de vendas para Planejamento Colaborativo por Cliente
# Encontrar o primeiro dia do mês atual
colunas_agrupadas = ['COD_GRUPO_CLIENTE', 'DESC_GRUPO_E_CLIENTE', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA']

hoje = datetime.today()
primeiro_dia_mes_atual = datetime(hoje.year, hoje.month, 1)

# Calcular o primeiro dia do mês de 6 meses atrás (excluindo mês atual)
primeiro_dia_6_meses_atras = (primeiro_dia_mes_atual - pd.DateOffset(months=6)).to_pydatetime()

# Filtrar apenas os últimos 6 meses (excluindo mês atual)
mask = (df_hist_vend_CLIENTE['PERIODO'] >= primeiro_dia_6_meses_atras) & (df_hist_vend_CLIENTE['PERIODO'] < primeiro_dia_mes_atual)
df_hist_vend_CLIENTE_ultimos_6_meses = df_hist_vend_CLIENTE.loc[mask].copy()

# Ordenar por data crescente
df_hist_vend_CLIENTE_ultimos_6_meses = df_hist_vend_CLIENTE_ultimos_6_meses.sort_values('PERIODO').reset_index(drop=True)

# DataFrame dos 3 meses mais recentes (últimos 3 meses do intervalo filtrado)
df_3_meses_mais_recentes = df_hist_vend_CLIENTE_ultimos_6_meses.copy()

# Identificar as 3 datas mais recentes (sem duplicar por linha)
meses_recentes = sorted(df_3_meses_mais_recentes['PERIODO'].unique())[-3:]

# Filtrar todas as linhas que pertencem a esses 3 meses
df_3_meses_mais_recentes = df_3_meses_mais_recentes[df_3_meses_mais_recentes['PERIODO'].isin(meses_recentes)].copy()

# Agrupa pelas colunas desejadas e calcula a média das colunas numéricas
df_3_meses_mais_recentes_media = df_3_meses_mais_recentes.groupby(colunas_agrupadas).mean(numeric_only=True).reset_index()

# Adicionar coluna MEDIA informando 'MÉDIA 3 MESES' na coluna
df_3_meses_mais_recentes_media['MEDIA'] = 'MÉDIA 3 MESES'

# Agrupamento fazendo média dos 6 meses
df_6_meses_mais_recentes_media = df_hist_vend_CLIENTE_ultimos_6_meses.copy()
df_6_meses_mais_recentes_media = df_6_meses_mais_recentes_media.groupby(colunas_agrupadas).mean(numeric_only=True).reset_index()

# Adicionar coluna MEDIA informando 'MÉDIA 6 MESES' na coluna
df_6_meses_mais_recentes_media['MEDIA'] = 'MÉDIA 6 MESES'

# Concatenar os DataFrames
df_media_vendas_CLIENTE = pd.concat([df_3_meses_mais_recentes_media, df_6_meses_mais_recentes_media], ignore_index=True)

# Pivotar a coluna MEDIA
df_media_vendas_CLIENTE = df_media_vendas_CLIENTE.pivot_table(
    index=colunas_agrupadas,
    columns='MEDIA',
    values='VOL_VENDA',
    aggfunc='sum',
    fill_value=0
).reset_index()

# Validar se as colunas 'MÉDIA 3 MESES' e 'MÉDIA 6 MESES' existem, caso contrário, criar com valor 0
if 'MÉDIA 3 MESES' not in df_media_vendas_CLIENTE.columns:
    df_media_vendas_CLIENTE['MÉDIA 3 MESES'] = 0.0
if 'MÉDIA 6 MESES' not in df_media_vendas_CLIENTE.columns:
    df_media_vendas_CLIENTE['MÉDIA 6 MESES'] = 0.0

# Gerar o arquivo CSV
df_media_vendas_CLIENTE.to_csv(
    pasta_input_painel / 'MEDIA_VENDA_KRONA_CLIENTE.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

del df_hist_vend_PRODUTO, df_hist_vend_CLIENTE, df_vendas_krona, produtos_a_eliminar
gc.collect()

print("✅ Bases de Vendas para Planejamento Colaborativo geradas com sucesso!")

# %%
# ============================================================
# 🧩 HISTÓRICO DOS MODELOS TESTADOS NO PROJETO
# ============================================================
#
# Ao longo do desenvolvimento do pipeline de previsão, diferentes
# abordagens estatísticas e de machine learning foram testadas
# com o objetivo de encontrar o melhor equilíbrio entre:
#
# - estabilidade
# - interpretabilidade
# - capacidade preditiva
#
#
# 1) Média Simples / Média 12 Meses
#    Descrição:
#    cálculo da média das vendas dos últimos 12 meses.
#
#    Objetivo:
#    criar um ponto de partida rápido para validar estabilidade
#    do histórico.
#
#    Vantagens:
#    - extremamente leve computacionalmente
#    - fácil interpretação
#
#    Limitações:
#    - não captura tendência de crescimento ou queda
#    - ignora padrões sazonais
#
#
# 2) Média Móvel Ponderada
#    Descrição:
#    média dos últimos 12 meses com pesos maiores para os meses
#    mais recentes.
#
#    Objetivo:
#    dar maior importância ao comportamento recente da série.
#
#    Vantagens:
#    - mais sensível a mudanças recentes
#    - mantém simplicidade computacional
#
#    Limitações:
#    - ainda não reconhece padrões sazonais completos
#    - pode reagir excessivamente a ruídos recentes
#
#
# 3) Sazonalidade Percentual Histórica
#    Descrição:
#    cálculo da participação média de cada mês no total anual
#    de vendas.
#
#    Objetivo:
#    reproduzir padrões sazonais característicos do negócio.
#
#    Vantagens:
#    - respeita picos e vales sazonais do histórico
#    - simples de explicar para usuários de negócio
#
#    Limitações:
#    - depende fortemente da qualidade do último ano
#    - não projeta volume total, apenas distribui valores
#
#
# 4) LightGBM
#    Descrição:
#    modelo de machine learning baseado em gradient boosting
#    aplicado sobre variáveis temporais.
#
#    Objetivo:
#    aprender padrões não lineares complexos presentes nas séries.
#
#    Vantagens:
#    - alto poder preditivo
#    - bom desempenho em bases amplas
#
#    Limitações:
#    - exige ajuste de hiperparâmetros
#    - pode superajustar em séries curtas
#
#
# 5) Regressão Linear
#    Descrição:
#    ajuste de tendência linear ao longo do tempo.
#
#    Objetivo:
#    capturar crescimento ou queda estruturais.
#
#    Vantagens:
#    - simples e robusto
#    - fácil de interpretar
#
#    Limitações:
#    - não captura sazonalidade
#    - pode falhar em séries muito voláteis
#
#
# 6) Holt-Winters
#    Descrição:
#    modelo clássico de suavização exponencial com componentes
#    de nível, tendência e sazonalidade.
#
#    Objetivo:
#    capturar comportamento temporal estruturado da série.
#
#    Vantagens:
#    - modelo consolidado em previsão de séries temporais
#    - equilibra tendência e sazonalidade
#
#    Limitações:
#    - sensível a séries muito curtas
#    - computacionalmente mais pesado em grandes volumes
#
#
# 7) Ensemble Estatístico (fase intermediária)
#    Descrição:
#    combinação ponderada de modelos estatísticos simples.
#
#    Objetivo:
#    estabilizar previsões mantendo coerência estatística.
#
#    Vantagens:
#    - redução de variabilidade
#    - maior robustez em cenários incertos
#
#    Limitações:
#    - baixa interpretabilidade do modelo final
#    - menor transparência para usuários de negócio
#
#
# ============================================================
# 📊 MODELOS DE PREVISÃO APLICADOS (VERSÃO FINAL)
# ============================================================
#
# O script final realiza previsão mensal por material e regional
# (COD_PROD, REGIONAL) utilizando múltiplos modelos estatísticos
# e de machine learning.
#
# Para cada série temporal individual, os modelos são comparados
# automaticamente e o modelo com menor erro de previsão é
# selecionado para gerar a projeção futura.
#
#
# ============================================================
# 🔎 ESTRUTURA DA PREVISÃO
# ============================================================
#
# Para cada combinação (COD_PROD, REGIONAL):
#
# 1) O histórico mensal de vendas é agregado.
#
# 2) O histórico é dividido em duas partes:
#    - treino: parte inicial da série
#    - validação: janela final do histórico (ex.: últimos 12 meses)
#
# 3) Cada modelo gera previsões para a janela de validação.
#
# 4) O erro de previsão é calculado utilizando:
#    - WAPE (Weighted Absolute Percentage Error), ou
#    - MAPE (Mean Absolute Percentage Error)
#
# 5) O modelo com menor erro é selecionado.
#
# 6) O modelo selecionado é utilizado para prever o horizonte
#    futuro definido em:
#    df_periodo_previsao["PERIODO_PROJECAO"]
#
#
# ============================================================
# 🤖 MODELOS UTILIZADOS NO SCRIPT
# ============================================================
#
# 1) Regressão Linear (Linear Regression)
#    Captura:
#    - tendência linear ao longo do tempo
#
#    Uso:
#    - séries com comportamento estável
#    - séries curtas
#
#    Força:
#    - extremamente rápida
#    - excelente baseline estatístico
#
#
# 2) Holt-Winters (Exponential Smoothing)
#    Captura:
#    - nível
#    - tendência
#    - sazonalidade
#
#    O script tenta automaticamente três configurações:
#    1. sazonalidade multiplicativa
#    2. sazonalidade aditiva
#    3. apenas tendência (Holt)
#
#    Uso:
#    - séries com estrutura temporal clara
#
#    Força:
#    - modelo clássico amplamente aceito em previsão
#
#
# 3) Random Forest Regressor
#    Modelo de machine learning baseado em árvores de decisão.
#
#    Features utilizadas:
#    - índice temporal
#    - mês
#    - ano
#
#    Captura:
#    - relações não lineares
#    - padrões complexos de comportamento
#
#    Força:
#    - robusto a ruído
#    - bom desempenho em séries irregulares
#
#
# 4) Gradient Boosting Regressor
#    Modelo de boosting sequencial de árvores.
#
#    Captura:
#    - padrões complexos
#    - mudanças de regime
#    - interações entre variáveis temporais
#
#    Força:
#    - alto poder preditivo
#    - frequentemente superior em cenários não lineares
#
#
# 5) LinearRegression_Fallback
#    Estratégia automática de contingência.
#
#    Ativado quando:
#    - histórico da série é muito curto
#    - modelos mais complexos falham
#
#    Objetivo:
#    garantir continuidade da geração de previsões sem
#    interromper o pipeline
#
#
# ============================================================
# 🧪 BACKTEST WALK-FORWARD
# ============================================================
#
# Após a escolha do modelo ideal para cada série, o script
# executa um backtest completo do tipo walk-forward.
#
# Neste processo:
#
# 1) O modelo é treinado progressivamente com dados históricos.
#
# 2) A cada etapa, o modelo gera previsões para os próximos
#    períodos.
#
# 3) As previsões são comparadas com os valores reais.
#
# Isso permite avaliar a capacidade real de generalização do
# modelo antes da projeção futura.
#
#
# ============================================================
# 📏 MÉTRICAS DE AVALIAÇÃO
# ============================================================
#
# APE — Absolute Percentage Error
#
# Fórmula:
# APE = |Real - Previsto| / Real
#
#
# MAPE_SKU
#
# Média do erro percentual absoluto ao longo do histórico
# da série.
#
# Essa métrica representa a qualidade histórica do modelo
# para cada combinação (COD_PROD, REGIONAL).
#
#
# ============================================================
# 📦 ESTRUTURA DO DATASET FINAL
# ============================================================
#
# O dataset final contém histórico + previsão futura no mesmo
# formato mensal.
#
# Principais colunas:
#
# - COD_PROD          : código do produto
# - REGIONAL          : região de venda
# - PERIODO           : período mensal
# - VOL_VENDA_REAL    : valor histórico ou previsão futura
# - MODELO_ESCOLHIDO  : modelo selecionado para previsão
# - PREVISAO_BACKTEST : previsões geradas no backtest
# - APE               : erro percentual absoluto por período
# - MAPE_SKU          : erro médio histórico da série
# - PREVISAO_FINAL    : previsão final consolidada
#
#
# ============================================================
# 📌 OBSERVAÇÃO METODOLÓGICA
# ============================================================
#
# O mesmo modelo que gera a previsão futura também é aplicado
# no backtest histórico.
#
# Isso garante que:
#
# - o modelo utilizado foi previamente validado
# - a projeção futura possui base estatística consistente
# - seja possível medir a confiabilidade da previsão para
#   cada série
# ============================================================

# %%
timer.iniciar()

# ===========================
# PROTECOES (Windows / Notebook)
# ===========================
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

# ============================================================
# PARAMETRO DE NUCLEOS (ajuste aqui)
# ============================================================
N_NUCLEOS = 8   # sua máquina
# N_NUCLEOS = 4 # cliente

PRINT_EVERY = 50

# ============================================================
# 3) MÉTRICA (WAPE ou MAPE)
# ============================================================
METRICA_USADA = "WAPE"

# ============================================================
# MODO TESTE
# ============================================================
MODO_TESTE_COD_PROD = False   # True = roda produtos no COD_PROD_TESTE | False = roda base completa
COD_PROD_TESTE = ["0116", "0156", "0733", "0839", "1307", "1331", "1812", "1822", "2382", "2383"]    # produto para testar quando MODO_TESTE_COD_PROD=True

def wape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denom = np.sum(np.abs(y_true))
    if denom == 0:
        return float(np.mean(np.abs(y_true - y_pred)))
    return float(np.sum(np.abs(y_true - y_pred)) / denom)

def safe_mape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = y_true != 0
    if mask.sum() == 0:
        return float(np.mean(np.abs(y_true - y_pred)))
    return float(mean_absolute_percentage_error(y_true[mask], y_pred[mask]))

def metric(y_true, y_pred) -> float:
    return wape(y_true, y_pred) if METRICA_USADA == "WAPE" else safe_mape(y_true, y_pred)

# ============================================================
# 4) MODELOS (ARIMA REMOVIDO)
# ============================================================
def pred_hw(y_train, steps):
    try:
        m = ExponentialSmoothing(
            y_train, trend="add", seasonal="multiplicative", seasonal_periods=12
        ).fit()
        return np.maximum(m.forecast(steps), 0)
    except Exception:
        try:
            m = ExponentialSmoothing(
                y_train, trend="add", seasonal="additive", seasonal_periods=12
            ).fit()
            return np.maximum(m.forecast(steps), 0)
        except Exception:
            m = ExponentialSmoothing(y_train, trend="add", seasonal=None).fit()
            return np.maximum(m.forecast(steps), 0)

def pred_lr(y_train, steps):
    y_train = np.asarray(y_train, dtype=float)
    t = np.arange(len(y_train)).reshape(-1, 1)
    lr = LinearRegression().fit(t, y_train)
    t_future = np.arange(len(y_train), len(y_train) + steps).reshape(-1, 1)
    return np.maximum(lr.predict(t_future), 0)

def _make_X(idx: pd.DatetimeIndex, start_time: int):
    t = np.arange(start_time, start_time + len(idx), dtype=np.int32)
    mes = idx.month.values.astype(np.int16)
    ano = idx.year.values.astype(np.int16)
    return np.column_stack([t, mes, ano])

def pred_rf(period_index_train, y_train, period_index_pred):
    X_train = _make_X(period_index_train, 0)
    rf = RandomForestRegressor(n_estimators=400, random_state=42, n_jobs=1)
    rf.fit(X_train, y_train)

    X_pred = _make_X(period_index_pred, len(period_index_train))
    return np.maximum(rf.predict(X_pred), 0)

def pred_gb(period_index_train, y_train, period_index_pred):
    X_train = _make_X(period_index_train, 0)
    gb = GradientBoostingRegressor(random_state=42)
    gb.fit(X_train, y_train)

    X_pred = _make_X(period_index_pred, len(period_index_train))
    return np.maximum(gb.predict(X_pred), 0)

def pred_intermitente(y, horizon):
    y = np.asarray(y, dtype=float)

    if len(y) == 0:
        return np.zeros(horizon)

    ultimos_12 = y[-12:] if len(y) >= 12 else y
    n_pos_12 = np.count_nonzero(ultimos_12 > 0)

    if n_pos_12 == 0:
        return np.zeros(horizon)

    pos_vendas = np.where(y > 0)[0]
    meses_sem_venda = len(y) - 1 - pos_vendas[-1]

    media_12 = float(np.mean(ultimos_12))

    # Venda única e antiga: não projeta recorrência
    if n_pos_12 <= 1 and meses_sem_venda >= 3:
        base = 0.0

    # Pouquíssimas vendas: usa média incluindo meses zerados
    elif n_pos_12 <= 2:
        base = media_12

    # Série intermitente: média dos últimos 12 com zeros
    else:
        base = media_12

    return np.repeat(max(base, 0.0), horizon)

def limitar_forecast(fc, y, fator_media=3.0, fator_p75=2.0):
    fc = np.asarray(fc, dtype=float)
    y = np.asarray(y, dtype=float)

    ultimos_12 = y[-12:] if len(y) >= 12 else y

    if len(ultimos_12) == 0:
        return np.maximum(fc, 0)

    media_12 = float(np.mean(ultimos_12))
    p75_12 = float(np.percentile(ultimos_12, 75))

    cap_media = media_12 * fator_media
    cap_p75 = p75_12 * fator_p75

    cap = max(cap_media, cap_p75)

    # Se histórico é praticamente zero, não deixa explodir
    if cap <= 0:
        cap = 0.0

    return np.minimum(np.maximum(fc, 0), cap)

# ============================================================
# WORKERS (joblib loky)
# ============================================================
def _worker_forecast_serie(cod_prod, regional, periodos_np, y_np, future_dates_np, horizon, janela_validacao):
    df_serie_periodos = pd.DatetimeIndex(periodos_np)
    y = y_np.astype(float)

    # histórico curto/intermitente -> média 12M com zeros
    n_pos = np.count_nonzero(y > 0)
    densidade = n_pos / len(y) if len(y) else 0

    if len(y) < 12 or n_pos < 4 or densidade < 0.35:
        fc = pred_intermitente(y, horizon)
        fc = limitar_forecast(fc, y)
        best = "Media12_Intermitente"

        registros_local = [
            [cod_prod, regional, pd.Timestamp(per), float(val), best]
            for per, val in zip(future_dates_np, fc)
        ]

        return registros_local, (cod_prod, regional), best

    J = min(janela_validacao, max(3, len(y)//3))
    y_train, y_val = y[:-J], y[-J:]
    idx_train, idx_val = df_serie_periodos[:-J], df_serie_periodos[-J:]

    scores = {}

    try:
        pred_val = pred_hw(y_train, J)
        scores["HoltWinters"] = metric(y_val, pred_val)
    except Exception:
        pass

    try:
        pred_val = pred_lr(y_train, J)
        scores["LinearRegression"] = metric(y_val, pred_val)
    except Exception:
        pass

    try:
        pred_val = pred_rf(idx_train, y_train, idx_val)
        scores["RandomForest"] = metric(y_val, pred_val)
    except Exception:
        pass

    try:
        pred_val = pred_gb(idx_train, y_train, idx_val)
        scores["GradientBoosting"] = metric(y_val, pred_val)
    except Exception:
        pass

    if not scores:
        best = "LinearRegression_Fallback"
        fc = pred_lr(y, horizon)
    else:
        best = min(scores.items(), key=lambda x: x[1])[0]
        if best == "HoltWinters":
            fc = pred_hw(y, horizon)
        elif best == "LinearRegression":
            fc = pred_lr(y, horizon)
        elif best == "RandomForest":
            fc = pred_rf(df_serie_periodos, y, pd.DatetimeIndex(future_dates_np))
        else:
            fc = pred_gb(df_serie_periodos, y, pd.DatetimeIndex(future_dates_np))


    fc = limitar_forecast(fc, y)

    registros_local = [
        [cod_prod, regional, pd.Timestamp(per), float(val), best]
        for per, val in zip(future_dates_np, fc)
    ]
    return registros_local, (cod_prod, regional), best

def _worker_backtest_serie(cod_prod, regional, periodos_np, y_np, best, min_treino, step_backtest):
    idx = pd.DatetimeIndex(periodos_np)
    y = y_np.astype(float)

    preds = np.full(len(y), np.nan, dtype=float)
    ape = np.full(len(y), np.nan, dtype=float)

    t = min_treino
    while t < len(y):
        y_train = y[:t]
        idx_train = idx[:t]

        steps = min(step_backtest, len(y) - t)
        idx_pred = idx[t:t + steps]

        try:
            if best == "HoltWinters":
                y_preds = pred_hw(y_train, steps)

            elif best in ("LinearRegression", "LinearRegression_Fallback"):
                y_preds = pred_lr(y_train, steps)

            elif best == "Media12_Intermitente":
                y_preds = pred_intermitente(y_train, steps)

            elif best == "RandomForest":
                y_preds = pred_rf(idx_train, y_train, idx_pred)

            elif best == "GradientBoosting":
                y_preds = pred_gb(idx_train, y_train, idx_pred)

            else:
                y_preds = pred_intermitente(y_train, steps)

        except Exception:
            y_preds = np.full(steps, np.mean(y_train) if len(y_train) else 0.0)

        for i_step in range(steps):
            pos = t + i_step
            preds[pos] = max(float(y_preds[i_step]), 0)

            if y[pos] != 0:
                ape[pos] = abs((y[pos] - preds[pos]) / y[pos])

        t += steps

    mask_pred = ~np.isnan(preds)

    if mask_pred.sum() == 0:
        mape_serie = np.nan
    else:
        mape_serie = metric(y[mask_pred], preds[mask_pred])

    return (cod_prod, regional), preds, ape, float(mape_serie), best

def completar_calendario_mensal(df_hist_base, ultimo_mes_hist):
    df_hist_base = df_hist_base.copy()
    df_hist_base["PERIODO"] = pd.to_datetime(df_hist_base["PERIODO"]).dt.to_period("M").dt.to_timestamp()

    partes = []

    for (cod_prod, regional), g in df_hist_base.groupby(["COD_PROD", "REGIONAL"], sort=False):
        g = g.sort_values("PERIODO")

        calendario = pd.date_range(
            start=g["PERIODO"].min(),
            end=ultimo_mes_hist,
            freq="MS"
        )

        g2 = (
            g.set_index("PERIODO")
             .reindex(calendario)
             .rename_axis("PERIODO")
             .reset_index()
        )

        g2["COD_PROD"] = cod_prod
        g2["REGIONAL"] = regional
        g2["VOL_VENDA"] = g2["VOL_VENDA"].fillna(0)

        partes.append(g2[["COD_PROD", "REGIONAL", "PERIODO", "VOL_VENDA"]])

    return pd.concat(partes, ignore_index=True)

# ============================================================
# MAIN
# ============================================================
def main():
    # PREVISAO ESTATISTICA — 4 MODELOS + MELHOR POR (COD_PROD, REGIONAL)
    print("🔄 Iniciando processo de previsão estatística...")

    # ============================================================
    # 0) CARREGAR df_vendas_krona DO PARQUET
    # ============================================================
    df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")
    print(f"📦 df_vendas_krona carregado | Linhas: {len(df_vendas_krona):,}")

    # ============================================================
    # FILTRO OPCIONAL PARA TESTE DE UM OU MAIS PRODUTOS
    # ============================================================
    if MODO_TESTE_COD_PROD:

        df_vendas_krona["COD_PROD"] = (
            df_vendas_krona["COD_PROD"]
            .astype(str)
            .str.strip()
        )

        # Aceita tanto um único código string quanto uma lista de códigos
        if isinstance(COD_PROD_TESTE, (list, tuple, set)):
            codigos_base = [str(cod).strip() for cod in COD_PROD_TESTE]
        else:
            codigos_base = [str(COD_PROD_TESTE).strip()]

        # Inclui versão original e versão sem zeros à esquerda
        codigos_teste = set()
        for cod in codigos_base:
            codigos_teste.add(cod)
            codigos_teste.add(cod.lstrip("0"))

        df_vendas_krona = df_vendas_krona[
            df_vendas_krona["COD_PROD"].isin(codigos_teste)
        ].copy()

        print(
            f"🧪 MODO TESTE ATIVO | Produtos base={codigos_base} | "
            f"Linhas após filtro: {len(df_vendas_krona):,}"
        )

        if df_vendas_krona.empty:
            raise ValueError(f"Nenhuma linha encontrada para COD_PROD em {codigos_base}")

    else:
        print("🏭 MODO COMPLETO ATIVO | Processando todos os produtos.")

    # ============================================================
    # 1) AGRUPAMENTO PADRÃO (COD_PROD + REGIONAL + PERIODO)
    # ============================================================
    df_group = (
        df_vendas_krona
        .groupby(["COD_PROD", "REGIONAL", "PERIODO"], as_index=False, sort=False)
        .agg(VOL_VENDA=("VOL_VENDA", "sum"))
        .sort_values(["COD_PROD", "REGIONAL", "PERIODO"])
    )

    print(
        f"📊 Dados agregados | Séries (COD_PROD,REGIONAL): "
        f"{df_group[['COD_PROD','REGIONAL']].drop_duplicates().shape[0]:,} | "
        f"Períodos: {df_group['PERIODO'].nunique():,}"
    )

    # ============================================================
    # 2) CALENDÁRIO FUTURO
    # ============================================================
    future_dates = pd.DatetimeIndex(
        df_periodo_previsao["PERIODO_PROJECAO"].drop_duplicates().sort_values()
    )

    if len(future_dates) == 0:
        raise ValueError("df_periodo_previsao['PERIODO_PROJECAO'] está vazio.")

    primeiro_mes_previsao = future_dates.min()

    # ============================================================
    # ÚLTIMO MÊS HISTÓRICO COMPLETO PELA DATA COTA
    # Regra:
    # - dia 21 inicia o próximo mês cota
    # - então o mês da data atual está completo quando rodar dia 21+
    # - qualquer PERIODO maior que esse é mês fatiado/incompleto
    # ============================================================
    hoje = pd.Timestamp.today().normalize()

    if hoje.day >= 21:
        ultimo_mes_hist = hoje.to_period("M").to_timestamp()
    else:
        ultimo_mes_hist = (hoje - pd.offsets.MonthBegin(1)).to_period("M").to_timestamp()

    df_hist_base = df_group[df_group["PERIODO"] <= ultimo_mes_hist].copy()

    if df_hist_base.empty:
        raise ValueError("Histórico vazio após corte pelo último mês completo da data cota.")

    print(
        f"📌 Corte histórico pela data cota | "
        f"Hoje: {hoje.date()} | "
        f"Último mês histórico consumido: {ultimo_mes_hist.date()}"
    )
    # ============================================================

    df_hist_base = completar_calendario_mensal(df_hist_base, ultimo_mes_hist)

    horizon = len(future_dates)

    print(
        f"🗓️ Horizonte de previsão | Meses: {horizon} | "
        f"{future_dates.min().date()} → {future_dates.max().date()}"
    )

    # ============================================================
    # 5) ESCOLHER MELHOR MODELO POR (COD_PROD, REGIONAL) + prever futuro
    # ============================================================
    JANELA_VALIDACAO = 12

    tasks = []
    for (cod_prod, regional), df_serie in df_hist_base.groupby(["COD_PROD", "REGIONAL"], sort=False):
        df_serie = df_serie.sort_values("PERIODO")
        periodos_np = df_serie["PERIODO"].to_numpy(dtype="datetime64[ns]")
        y_np = df_serie["VOL_VENDA"].to_numpy(dtype=float)
        tasks.append((cod_prod, regional, periodos_np, y_np))

    total_series = len(tasks)
    print(f"🚀 Iniciando previsão por série | Total: {total_series:,}")

    future_dates_np = future_dates.to_numpy(dtype="datetime64[ns]")

    # executa em paralelo e coleta resultados
    t0 = time.time()
    registros = []
    best_model_por_serie = {}

    results = Parallel(n_jobs=N_NUCLEOS, backend="loky", batch_size="auto", verbose=0)(
        delayed(_worker_forecast_serie)(
            cod_prod, regional, periodos_np, y_np, future_dates_np, horizon, JANELA_VALIDACAO
        )
        for (cod_prod, regional, periodos_np, y_np) in tasks
    )

    for i, (registros_local, key, best) in enumerate(results, start=1):
        registros.extend(registros_local)
        best_model_por_serie[key] = best

        if i == 1 or i % PRINT_EVERY == 0 or i == total_series:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0.0
            eta = (total_series - i) / rate if rate > 0 else float("inf")
            cod_prod, regional = key
            print(
                f"   ▶️ Processando série {i}/{total_series} | COD_PROD={cod_prod} | REGIONAL={regional} | "
                f"Decorrido: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min"
            )

    print("🧩 Finalizando consolidação de resultados (forecast futuro)...")

    df_forecast = pd.DataFrame(
        registros,
        columns=["COD_PROD", "REGIONAL", "PERIODO", "VOL_VENDA_REAL", "MODELO_ESCOLHIDO"]
    ).sort_values(["COD_PROD", "REGIONAL", "PERIODO"]).reset_index(drop=True)

    # ============================================================
    # 6) SAÍDA FINAL (histórico + projeção)
    # ============================================================
    df_final_hist = df_hist_base.rename(columns={"VOL_VENDA": "VOL_VENDA_REAL"}).copy()
    df_final_hist["MODELO_ESCOLHIDO"] = np.nan

    df_forecast_estatistico_krona = pd.concat([df_final_hist, df_forecast], ignore_index=True)
    df_forecast_estatistico_krona = df_forecast_estatistico_krona.sort_values(
        ["COD_PROD", "REGIONAL", "PERIODO", "MODELO_ESCOLHIDO"],
        na_position="first"
    ).reset_index(drop=True)

    print("📦 Dataset base montado (histórico + futuro).")

    # ============================================================
    # 6.1) BACKTEST COMPLETO (walk-forward) com o MESMO modelo do futuro
    # ============================================================
    print("🧪 Iniciando backtest completo (walk-forward) por série...")

    STEP_BACKTEST = 6
    df_forecast_estatistico_krona["PREVISAO_BACKTEST"] = np.nan
    df_forecast_estatistico_krona["MODELO_BACKTEST"] = np.nan
    df_forecast_estatistico_krona["APE"] = np.nan
    df_forecast_estatistico_krona["MAPE_SKU"] = np.nan

    MIN_TREINO = 12

    df_hist_all = df_forecast_estatistico_krona[df_forecast_estatistico_krona["MODELO_ESCOLHIDO"].isna()].copy()
    df_hist_all = df_hist_all.sort_values(["COD_PROD", "REGIONAL", "PERIODO"])

    total_series_bt = df_hist_all[["COD_PROD","REGIONAL"]].drop_duplicates().shape[0]
    print(f"🔎 Backtest completo | Total séries: {total_series_bt:,} | MIN_TREINO={MIN_TREINO}")

    mask_hist = df_forecast_estatistico_krona["MODELO_ESCOLHIDO"].isna()

    idx_map_hist = (
        df_forecast_estatistico_krona.loc[mask_hist]
        .sort_values(["COD_PROD", "REGIONAL", "PERIODO"])
        .groupby(["COD_PROD", "REGIONAL"], sort=False)
        .groups
    )

    tasks_bt = []
    for (cod_prod, regional), d in df_hist_all.groupby(["COD_PROD","REGIONAL"], sort=False):
        d = d.sort_values("PERIODO")
        periodos_np = d["PERIODO"].to_numpy(dtype="datetime64[ns]")
        y_np = d["VOL_VENDA_REAL"].to_numpy(dtype=float)
        best = best_model_por_serie.get((cod_prod, regional), "LinearRegression_Fallback")
        tasks_bt.append((cod_prod, regional, periodos_np, y_np, best))

    t1 = time.time()
    results_bt = Parallel(n_jobs=N_NUCLEOS, backend="loky", batch_size="auto", verbose=0)(
        delayed(_worker_backtest_serie)(
            cod_prod, regional, periodos_np, y_np, best, MIN_TREINO, STEP_BACKTEST
        )
        for (cod_prod, regional, periodos_np, y_np, best) in tasks_bt
    )

    mape_por_serie_final = {}

    for i, (key, preds, ape, mape_serie, best) in enumerate(results_bt, start=1):
        cod_prod, regional = key
        mape_por_serie_final[key] = mape_serie

        idx_rows = idx_map_hist.get((cod_prod, regional))
        if idx_rows is not None:
            df_forecast_estatistico_krona.loc[idx_rows, "PREVISAO_BACKTEST"] = preds
            df_forecast_estatistico_krona.loc[idx_rows, "MODELO_BACKTEST"] = best
            df_forecast_estatistico_krona.loc[idx_rows, "APE"] = ape
            df_forecast_estatistico_krona.loc[idx_rows, "MAPE_SKU"] = mape_serie

        if i == 1 or i % PRINT_EVERY == 0 or i == total_series_bt:
            elapsed = time.time() - t1
            rate = i / elapsed if elapsed > 0 else 0.0
            eta = (total_series_bt - i) / rate if rate > 0 else float("inf")
            print(
                f"   ▶️ Backtest série {i}/{total_series_bt} | COD_PROD={cod_prod} | REGIONAL={regional} | "
                f"Decorrido: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min"
            )

    # ============================================================
    # preencher MAPE_SKU também no futuro (por série)  <<< CORRIGIDO
    # ============================================================
    print("🧾 Preenchendo MAPE_SKU no futuro (por série)...")

    mape_series = pd.Series(
        df_forecast_estatistico_krona.set_index(["COD_PROD","REGIONAL"]).index.map(mape_por_serie_final),
        index=df_forecast_estatistico_krona.index,
        dtype=float
    )

    df_forecast_estatistico_krona["MAPE_SKU"] = df_forecast_estatistico_krona["MAPE_SKU"].fillna(mape_series)

    # ============================================================
    # LIMPAR BACKTEST NAS LINHAS FUTURAS
    # Backtest só deve existir no histórico.
    # Futuro deve manter apenas previsão final e MAPE_SKU da série.
    # ============================================================
    mask_futuro = df_forecast_estatistico_krona["PERIODO"].isin(future_dates)

    df_forecast_estatistico_krona.loc[
        mask_futuro,
        ["PREVISAO_BACKTEST", "MODELO_BACKTEST", "APE"]
    ] = np.nan


    # PREVISAO_FINAL: no futuro usa VOL_VENDA_REAL (que é o forecast),
    # no histórico usa PREVISAO_BACKTEST
    df_forecast_estatistico_krona["PREVISAO_FINAL"] = np.where(
        df_forecast_estatistico_krona["PERIODO"].isin(future_dates),
        df_forecast_estatistico_krona["VOL_VENDA_REAL"],
        df_forecast_estatistico_krona["PREVISAO_BACKTEST"]
    )

    print("✅ Backtest completo concluído (histórico preenchido).")

    # ============================================================
    # 7) SALVAR CSV
    # Se o arquivo padrão estiver aberto/bloqueado, salva com complemento no nome
    # ============================================================
    if MODO_TESTE_COD_PROD:
        arquivo_saida = pasta_staging_parquet / "df_forecast_estatistico_krona_TESTE.csv"
    else:
        arquivo_saida = pasta_staging_parquet / "df_forecast_estatistico_krona.csv"

    try:
        df_forecast_estatistico_krona.to_csv(
            arquivo_saida,
            sep=";",
            encoding="utf-8-sig",
            index=False,
            decimal=",",
            float_format="%.2f"
        )

        print(f"✅ Finalizado e salvo: {arquivo_saida}")

    except PermissionError:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        arquivo_saida_alt = arquivo_saida.with_name(
            f"{arquivo_saida.stem}_BLOQUEADO_{timestamp}{arquivo_saida.suffix}"
        )

        df_forecast_estatistico_krona.to_csv(
            arquivo_saida_alt,
            sep=";",
            encoding="utf-8-sig",
            index=False,
            decimal=",",
            float_format="%.2f"
        )

        print(f"⚠️ Arquivo padrão estava aberto/bloqueado: {arquivo_saida}")
        print(f"✅ Salvo com nome alternativo: {arquivo_saida_alt}")

# EXECUTAR (ipynb)
try:
    main()
finally:
    timer.finalizar()

# %%
# ============================================================
# DESAGREGAÇÃO DO FORECAST ESTATÍSTICO (HISTÓRICO + FUTURO)
# SAÍDA ÚNICA: df_prev_krona.parquet (HIST + FUT)
# ============================================================

# =========================
# START
# =========================
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")
print("🔄 Iniciando processo de desagregação (histórico + futuro)...")
print(f"📦 df_vendas_krona em memória | Linhas: {len(df_vendas_krona):,}".replace(",", "."))

# =========================
# LER FORECAST
# =========================
print("📊 Lendo arquivo de previsão estatística salvo...")

arquivo_forecast = pasta_staging_parquet / "df_forecast_estatistico_krona.csv"

df_forecast_estatistico_krona = pd.read_csv(
    arquivo_forecast,
    sep=";",
    encoding="utf-8-sig",
    decimal=",",
    dtype={"COD_PROD": str},
    dayfirst=True
)

df_forecast_estatistico_krona["PERIODO"] = pd.to_datetime(df_forecast_estatistico_krona["PERIODO"])
df_forecast_estatistico_krona = df_forecast_estatistico_krona[df_forecast_estatistico_krona["PREVISAO_FINAL"].notna()].copy()

df_vendas_krona["PERIODO"] = pd.to_datetime(df_vendas_krona["PERIODO"])

# Eliminar colunas desnecessárias no df_forecast_estatistico_krona
df_forecast_estatistico_krona = df_forecast_estatistico_krona.drop(
    columns=["MODELO_ESCOLHIDO", "PREVISAO_BACKTEST", "MODELO_BACKTEST", "APE", "MAPE_SKU", "VOL_VENDA_REAL"]
)

# Manter somente PERIODO conforme variável df_periodo_previsao
df_forecast_estatistico_krona = df_forecast_estatistico_krona[
    df_forecast_estatistico_krona["PERIODO"].isin(
        pd.to_datetime(df_periodo_previsao["PERIODO_PROJECAO"].unique())
    )
].copy().reset_index(drop=True)

n_combinacoes = df_forecast_estatistico_krona[["COD_PROD", "REGIONAL"]].drop_duplicates().shape[0]
n_periodos = df_forecast_estatistico_krona["PERIODO"].nunique()
p_min = df_forecast_estatistico_krona["PERIODO"].min()
p_max = df_forecast_estatistico_krona["PERIODO"].max()

print(f"📊 Forecast carregado | Combinações: {n_combinacoes:,} | Períodos: {n_periodos}".replace(",", "."))
print(f"🗓️ Horizonte de previsão | Meses: {n_periodos} | {p_min.date()} → {p_max.date()}")

# =========================
# DEFINIR HISTÓRICO PARA DESAGREGAÇÃO DO FORECAST
# =========================
print("📦 Montando base df_prev_krona (histórico)...")
meses_hist_desagregacao = 12

# df_prev_krona deve ser cópia de df_vendas_krona, filtrando PERIODO pela variavel meses_hist_desagregacao, retornar os ultimos 12 que constam no arquivo df_vendas_krona
periodos_disponiveis = sorted(df_vendas_krona["PERIODO"].unique())
periodos_para_manter = periodos_disponiveis[-meses_hist_desagregacao:]
df_prev_krona = df_vendas_krona[df_vendas_krona["PERIODO"].isin(periodos_para_manter)].copy().reset_index(drop=True)

# Agrupar dados somando as VOL_VENDA
chaves_desagregacao = [
    "EMPRESA","COD_CLIENTE","NOME_CLIENTE","COD_GRUPO_CLIENTE","DESC_GRUPO_E_CLIENTE",
    "COD_PROD","DESC_PRODUTO","FAMILIA","LINHA","REGIONAL","REGIONAL_GESTOR"
]

chaves_sem_periodo = chaves_desagregacao[:]
df_prev_krona = df_prev_krona.groupby(
    chaves_desagregacao,
    as_index=False
).agg({'VOL_VENDA': 'sum'}).reset_index(drop=True)

print("🚀 Gerando percentuais de desagregação")

# Criar coluna TOTAL_VOL_VENDA por COD_PROD e REGIONAL
total_vol_venda_por_prod = df_prev_krona.groupby(['COD_PROD', 'REGIONAL'])['VOL_VENDA'].transform('sum')
df_prev_krona["TOTAL_VOL_VENDA"] = total_vol_venda_por_prod

# Criar coluna PERC_DESAGR
df_prev_krona["PERC_DESAGR"] = df_prev_krona["VOL_VENDA"] / df_prev_krona["TOTAL_VOL_VENDA"]

df_prev_explodido = (
    df_prev_krona
    .merge(
        df_forecast_estatistico_krona,
        on=["COD_PROD", "REGIONAL"],
        how="inner"   # só explode onde existe forecast
    )
)

print("🚀 Aplicando percentuais para desagregar o forecast estatístico...")

df_prev_explodido["VOL_PREV"] = (
    df_prev_explodido["PREVISAO_FINAL"] * df_prev_explodido["PERC_DESAGR"]
)

df_prev_krona = df_prev_explodido.copy()

# Carregar df_dim_peso_unit_vendas
df_dim_peso_unit_vendas = pd.read_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet")

# Adicionar coluna PESO_UNITÁRIO
df_prev_krona = df_prev_krona.merge(
    df_dim_peso_unit_vendas[["COD_PROD", "PESO_UNIT"]],
    on=["COD_PROD"],
    how="left"
)


# Criar coluna QTD_PREV
df_prev_krona["QTD_PREV"] = df_prev_krona["VOL_PREV"] / df_prev_krona["PESO_UNIT"]

# Selecionar colunas finais e ordenar
colunas_finais = [
    "EMPRESA","COD_CLIENTE","NOME_CLIENTE","COD_GRUPO_CLIENTE", "DESC_GRUPO_E_CLIENTE","COD_PROD",
    "DESC_PRODUTO","FAMILIA", "LINHA","REGIONAL","REGIONAL_GESTOR","PERIODO", "VOL_PREV", "QTD_PREV"
]

df_prev_krona = df_prev_krona[colunas_finais].copy()

# FIXME
del df_prev_explodido, df_forecast_estatistico_krona, df_vendas_krona, df_dim_peso_unit_vendas
gc.collect()

print(f"✅ df_prev_krona pronto | Linhas: {len(df_prev_krona):,}".replace(",", "."))

print("💾 Salvando df_prev_krona completo em Parquet...")

df_prev_krona.to_parquet(
    pasta_staging_parquet / "df_prev_krona.parquet",
    engine="pyarrow",
    compression="snappy",
    index=False
)

print("✅ Parquet salvo com sucesso: df_prev_krona.parquet")

del df_prev_krona
gc.collect()


# %%
# Separar a df_forecast_vendas_krona em dois dataframes:
# df_forecast_vendas_krona_CLIENTE: clientes que terão planejamento de demanda
# df_forecast_vendas_krona_PRODUTO: produtos que terão planejamento de demanda

print("🔄 Gerando arquivos Forecast para Painel S&OP...")

# Ler df_prev_krona do Parquet
df_prev_krona = pd.read_parquet(pasta_staging_parquet / "df_prev_krona.parquet")

df_forecast_vendas_krona = df_prev_krona[
    df_prev_krona["PERIODO"].isin(df_periodo_previsao["PERIODO_PROJECAO"])
].copy()

# Se lista_clientes_plan_demanda estiver vazio → todos são PRODUTO
if lista_clientes_plan_demanda and len(lista_clientes_plan_demanda) > 0:
    df_forecast_vendas_krona['NIVEL_PLAN_DEMANDA'] = np.where(
        df_forecast_vendas_krona['COD_GRUPO_CLIENTE'].isin(lista_clientes_plan_demanda),
        'CLIENTE',
        'PRODUTO'
    )
else:
    # Se não existe cliente para plan. demanda → tudo produto
    df_forecast_vendas_krona['NIVEL_PLAN_DEMANDA'] = 'PRODUTO'
    
# Separar os dataframes com cópia explícita
df_forecast_vendas_krona_CLIENTE = df_forecast_vendas_krona[df_forecast_vendas_krona['NIVEL_PLAN_DEMANDA'] == 'CLIENTE'].copy()
df_forecast_vendas_krona_PRODUTO = df_forecast_vendas_krona[df_forecast_vendas_krona['NIVEL_PLAN_DEMANDA'] == 'PRODUTO'].copy()

# Eliminar coluna NIVEL_PLAN_DEMANDA
df_forecast_vendas_krona_CLIENTE.drop(columns=['NIVEL_PLAN_DEMANDA'], inplace=True)
df_forecast_vendas_krona_CLIENTE.reset_index(drop=True, inplace=True)
df_forecast_vendas_krona_PRODUTO.drop(columns=['NIVEL_PLAN_DEMANDA'], inplace=True)
df_forecast_vendas_krona_PRODUTO.reset_index(drop=True, inplace=True)

#========================================================
# PADRONIZANDO TEMPLATE df_forecast_vendas_krona_PRODUTO
#========================================================

# Eliminar colunas NOME_CLIENTE E DESC_GRUPO_E_CLIENTE
df_forecast_vendas_krona_PRODUTO.drop(columns=['COD_CLIENTE', 'NOME_CLIENTE', 'COD_GRUPO_CLIENTE', 'DESC_GRUPO_E_CLIENTE'], inplace=True)

# Sumarizar df_forecast_vendas_krona_PRODUTO por EMPRESA, COD_PROD, DESC_PRODUTO, FAMILIA, LINHA, REGIONAL, PERIODO
df_forecast_vendas_krona_PRODUTO = df_forecast_vendas_krona_PRODUTO.groupby(
    ['EMPRESA', 'COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'REGIONAL', 'REGIONAL_GESTOR', 'PERIODO'],
    as_index=False
).agg({'VOL_PREV': 'sum'}).reset_index(drop=True)

# Gerar arquivos em PARQUET
df_forecast_vendas_krona_PRODUTO.to_parquet(pasta_staging_parquet / 'df_forecast_vendas_krona_PRODUTO.parquet', index=False)
df_forecast_vendas_krona_CLIENTE.to_parquet(pasta_staging_parquet / 'df_forecast_vendas_krona_CLIENTE.parquet', index=False)

# 📤 Exportação de Dados Forecast para Planejamento Colaborativo
# 📊 Nível de agregação: REGIONAL_GESTOR, 'REGIONAL', 'FAMILIA', 'PERIODO'

df_Forecast_PRODUTO = df_forecast_vendas_krona_PRODUTO.groupby(
    ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO'],
    as_index=False
).agg({'VOL_PREV': 'sum'}).reset_index(drop=True)

df_Forecast_PRODUTO.to_csv(
    pasta_input_painel / 'FORECAST_KRONA_AGREGADO.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

print("✅ Arquivo FORECAST_KRONA_AGREGADO.csv gerado com sucesso!")

#========================================================
# PADRONIZANDO TEMPLATE df_forecast_vendas_krona_CLIENTE
#========================================================

# 📤 Exportação de Dados Forecast para Planejamento Colaborativo
# 📊 Nível de agregação: REGIONAL_GESTOR, 'REGIONAL', 'COD_GRUPO_CLIENTE, DESC_GRUPO_E_CLIENTE, FAMILIA e PERIODO

df_Forecast_CLIENTE = df_forecast_vendas_krona_CLIENTE.groupby(
    ['REGIONAL_GESTOR', 'REGIONAL', 'COD_GRUPO_CLIENTE', 'DESC_GRUPO_E_CLIENTE', 'FAMILIA', 'PERIODO'],
    as_index=False
).agg({'VOL_PREV': 'sum'}).reset_index(drop=True)

df_Forecast_CLIENTE.to_csv(
    pasta_input_painel / 'FORECAST_KRONA_CLIENTE.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

print("✅ Arquivo FORECAST_KRONA_CLIENTE.csv gerado com sucesso!")

del df_forecast_vendas_krona_PRODUTO, df_forecast_vendas_krona_CLIENTE, df_prev_krona
gc.collect()

timer.finalizar()
print("🎯 Processo concluído com sucesso!")


