# %% [markdown]
# #### 01. CARREGANDO MAPEAMENTO DE PASTAS E IMPORTS

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
pasta_hist_planos = caminho_base.parent / '04_HISTORICO_PLANOS' # Armazena arquivos históricos de planos de demanda

print("✅ Mapeamento de pastas concluído com sucesso!")

# %% [markdown]
# #### 02. CARREGANDO DADOS DO ARQUIVO KRONA_REGRAS

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

produtos_a_eliminar = df_produtos_eliminar[['COD_PROD']].drop_duplicates().reset_index(drop=True)

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
#---------------Carregar DIRECIONA_CLIENTES_REGIONAL--------------------#
#-----------------------------------------------------------------------#
guia_excel = 'DIRECIONA_CLIENTES_REGIONAL'
df_direc_cli_regional = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine', dtype={'COD_GRUPO_CLIENTE': str, 'COD_CLIENTE': str})
df_direc_cli_regional = df_direc_cli_regional[df_direc_cli_regional['COD_CLIENTE'].notna()].reset_index(drop=True)

#-----------------------------------------------------------------------#
#---------------Carregar PERIODO_ORCAMENTO-------------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'PERIODO_ORCAMENTO'
df_periodo_orcamento = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine')
df_periodo_orcamento = df_periodo_orcamento[df_periodo_orcamento['PERIODO_PROJECAO'].notna()].reset_index(drop=True)
df_periodo_orcamento = df_periodo_orcamento.drop_duplicates(subset=['PERIODO_PROJECAO'])

# Classificar o df_periodo_orcamento em ordem crescente de PERIODO_PROJECAO
df_periodo_orcamento = df_periodo_orcamento.sort_values(by='PERIODO_PROJECAO').reset_index(drop=True)

print("✅ Importação e tratamento de dados do arquivo KRONA_REGRAS, concluídos com sucesso!")

# %% [markdown]
# #### 03. ELIMINANDO DUPLICATAS DO DIM_CLIENTES_KRONA

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

# %% [markdown]
# #### 04. ORGANIZANDO DIM_PRODUTOS_KRONA PARA UTILIZAR PESOS UNITÁRIOS

# %%
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

# %% [markdown]
# #### 05. CARREGAR FATO_VENDAS_KRONA, ORGANIZANDO DADOS, GERANDO DATA COTA E GERANDO A CAMADA SILVER

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
    SUM(TRY_CAST(Qtd_Peso_Venda AS DOUBLE)) AS VOL_VENDA,
    SUM(TRY_CAST(Val_Venda AS DOUBLE)) AS VAL_VENDA
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
    TRIM(Des_Grupo_Segmento) AS GRUPO_SEGMENTO,
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
    c.GRUPO_SEGMENTO,
    f.Cod_Produto AS COD_PROD,
    p.Des_Produto AS DESC_PRODUTO,
    CAST(p.Cod_Familia AS VARCHAR) || ' - ' || p.Des_Familia AS FAMILIA,
    CAST(p.Cod_Linha   AS VARCHAR) || ' - ' || p.Des_Linha   AS LINHA,
    v1.Des_Regiao AS REGIAO_CLIENTE,
    v2.Des_Regiao AS REGIAO_MOVIMENTO,
    f.PERIODO,
    f.QTD_VENDA,
    f.VOL_VENDA,
    f.VOL_VENDA / f.QTD_VENDA AS PESO_UNIT,
    f.VAL_VENDA
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
  GRUPO_SEGMENTO,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
  PESO_UNIT,
  REGIAO_CLIENTE,
  REGIAO_MOVIMENTO,
  PERIODO,
  QTD_VENDA,
  VOL_VENDA,
  VAL_VENDA
FROM final
"""
df_vendas_krona_silver = duckdb.query(sql).to_df()

# Salvar em parquet
df_vendas_krona_silver.to_parquet(pasta_staging_parquet / "df_vendas_krona_silver.parquet", index=False)

del df_vendas_krona_silver
gc.collect()

print("✅ Carregamento de df_vendas_krona_silver concluído com sucesso!")

# %% [markdown]
# #### 06. CARREGAR CAMADA SILVER, DEFINIR REGIONAIS, ORGANIZAR DADOS FINAIS E GERAR CAMADA GOLD

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
      -- Override por cliente
      WHEN d.REGIONAL IS NOT NULL AND d.REGIONAL <> '' THEN d.REGIONAL
      
      -- Se SEGMENTO contém CONSTRUTORA ou INSTALADOR => usa de-para
      WHEN b.SEG_UP LIKE '%CONSTRUTORA%' OR b.SEG_UP LIKE '%INSTALADOR%'
        THEN COALESCE(m."REGIONAL ATUALIZADA", b.RC_FIX)

      -- Regras TELEVENDAS
      WHEN b.RC='TELEVENDAS' AND b.RM='TELEVENDAS' THEN 'TELEVENDAS'
      WHEN b.RC<>'TELEVENDAS' AND b.RM='TELEVENDAS' THEN 'TELEVENDAS'
      WHEN b.RC='TELEVENDAS' AND b.RM<>'TELEVENDAS' THEN b.RM
      ELSE b.RC_FIX
    END AS REGIONAL
  FROM base b
  LEFT JOIN map_reg m
    ON m."REGIONAL BASE" = b.REGIAO_CLIENTE
  LEFT JOIN direc_cli_regional d
    ON d.COD_CLIENTE = b.COD_CLIENTE
)

-- ============================================================
-- Resultado final consolidado
-- Agora agregado por GRUPO_SEGMENTO
-- e renomeado para SEGMENTO
-- ============================================================
SELECT
  EMPRESA,
--  COD_CLIENTE,
--  NOME_CLIENTE,
--  COD_GRUPO_CLIENTE,
--  DESC_GRUPO_E_CLIENTE,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
  REGIONAL,
--  REGIAO_CLIENTE,
--  REGIAO_MOVIMENTO,
  GRUPO_SEGMENTO AS SEGMENTO,
  PERIODO,
  SUM(QTD_VENDA) AS QTD_VENDA,
  SUM(VOL_VENDA) AS VOL_VENDA,
  SUM(VAL_VENDA) AS VAL_VENDA
FROM ajuste
-- WHERE REGIONAL IS NOT NULL AND REGIONAL <> ''
GROUP BY
  EMPRESA,
--  COD_CLIENTE,
--  NOME_CLIENTE,
--  COD_GRUPO_CLIENTE,
--  DESC_GRUPO_E_CLIENTE,
  COD_PROD,
  DESC_PRODUTO,
  FAMILIA,
  LINHA,
--  REGIAO_MOVIMENTO,
--  REGIAO_CLIENTE,
  REGIONAL,
  GRUPO_SEGMENTO,
  PERIODO
"""

# Executa no DuckDB
df_vendas_krona_gold = duckdb.query(sql).to_df()

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
    "COD_PROD",
    "DESC_PRODUTO",
    "FAMILIA",
    "LINHA",
    "REGIONAL",
    "REGIONAL_GESTOR",
    "SEGMENTO",
    "PERIODO",
    "QTD_VENDA",
    "VOL_VENDA",
    "VAL_VENDA"
]

df_vendas_krona_gold = df_vendas_krona_gold[colunas_ordenadas]

# Conforme alinhado com Karol, excluir REGIONAL = DIRETO KRONA
df_vendas_krona_gold = df_vendas_krona_gold[df_vendas_krona_gold["REGIONAL"] != "DIRETO KRONA"]

# Salvar df_vendas_krona_gold em Parquet para salvar as alterações, filtros e regras aplicadas no histórico, otimizando memória e garantindo rastreabilidade
df_vendas_krona_gold.to_parquet(pasta_staging_parquet / "df_vendas_krona_gold.parquet", index=False)

del df_vendas_krona_gold
gc.collect()

print("✅ Organização de Regionais e Inserção de Regional Gestor na df_vendas_krona_gold concluídos com sucesso!")

# %% [markdown]
# #### 07. ELIMINAR DA BASE DE VENDAS, OS PRODUTOS APONTADOS NO KRONAS_REGRAS

# %%
# Aplicar produtos a eliminar no df_vendas_krona_gold, e excluir os produtos listados na variavel produtos_a_eliminar vinda do arquivo de regras de negócio
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona_gold.parquet")
lista_produtos_eliminar = set(produtos_a_eliminar['COD_PROD'])
df_vendas_krona = df_vendas_krona[~df_vendas_krona['COD_PROD'].isin(lista_produtos_eliminar)]
df_vendas_krona.to_parquet(pasta_staging_parquet / "df_vendas_krona.parquet", index=False)

del df_vendas_krona
gc.collect()

print("✅ Eliminação de produtos concluída!")

# %% [markdown]
# #### 08. INCREMENTAR HIST. VENDAS COM DADOS DO ULTIMO CICLO S&OP PARA COMPLEMENTAR O ANO ATUAL

# %%
# -----------------------------------------------------------------------------
# Incrementar df_vendas_krona com dados dos meses definidos e direcionados no KRONA_REGRAS, para garantir que o df_vendas_krona contenha todos os meses necessários para a previsão de demanda
# -----------------------------------------------------------------------------

# Carregar plano de demanda de outros ciclos
df_plano_sop_regional = pd.read_parquet(pasta_hist_planos / "BD_PLANO_AGREGADO_PAINEL_REGIONAL.parquet")
df_plano_sop_cliente = pd.read_parquet(pasta_hist_planos / "BD_PLANO_AGREGADO_PAINEL_CLIENTE.parquet")

# Filtrar df_periodo_orcamento pela coluna TIPO_INFORMACAO = "SOP", e gerar lista para filtrar outro dataframe com a coluna PERIODO_PROJECAO
df_periodo_orcamento_sop = df_periodo_orcamento[df_periodo_orcamento["TIPO_INFORMACAO"] == "SOP"].copy()
lista_periodo_projecao_sop = df_periodo_orcamento_sop["PERIODO_PROJECAO"].tolist()

# Manter somente a ultima versão de plano de demanda Regional
# Identificar CICLO e REVISAO da última linha
ultimo_ciclo = df_plano_sop_regional.iloc[-1]["CICLO"]
ultima_revisao = df_plano_sop_regional.iloc[-1]["REVISAO"]

# Manter somente o último plano no próprio dataframe
df_plano_sop_regional = df_plano_sop_regional.loc[
    (df_plano_sop_regional["CICLO"] == ultimo_ciclo) &
    (df_plano_sop_regional["REVISAO"] == ultima_revisao) &
    (df_plano_sop_regional["PERIODO"].isin(lista_periodo_projecao_sop))
].copy()

# Manter somente a ultima versao de plano de demanda Cliente
if not df_plano_sop_cliente.empty:

    ultimo_ciclo_cliente = df_plano_sop_cliente.iloc[-1]["CICLO"]
    ultima_revisao_cliente = df_plano_sop_cliente.iloc[-1]["REVISAO"]

    df_plano_sop_cliente = df_plano_sop_cliente.loc[
        (df_plano_sop_cliente["CICLO"] == ultimo_ciclo_cliente) &
        (df_plano_sop_cliente["REVISAO"] == ultima_revisao_cliente) &
        (df_plano_sop_cliente["PERIODO"].isin(lista_periodo_projecao_sop))
    ].copy()

# Colunas que preciso manter nos arquivos
colunas_manter = ["REGIONAL_GESTOR", "REGIONAL", "FAMILIA", "PERIODO", "VALOR"]
df_plano_sop_regional = df_plano_sop_regional[colunas_manter].copy()
df_plano_sop_cliente = df_plano_sop_cliente[colunas_manter].copy()

# Unificar os dois dataframes de plano de demanda (Regional e Cliente) em um único dataframe
df_plano_sop = pd.concat([df_plano_sop_regional, df_plano_sop_cliente], ignore_index=True)

# Agregar os valores de VALOR por REGIONAL_GESTOR, REGIONAL, FAMILIA e PERIODO
df_plano_sop = df_plano_sop.groupby(["REGIONAL_GESTOR", "REGIONAL", "FAMILIA", "PERIODO"], as_index=False)["VALOR"].sum()

# Carregar df_vendas_krona do parquet
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")

# Filtrar ultimos 12 meses do df_vendas_krona, que base histórica para desagregação do S&OP
ultimo_mes_vendas = df_vendas_krona["PERIODO"].max()
primeiro_mes_vendas = ultimo_mes_vendas - pd.DateOffset(months=12)
df_vendas_krona_base_desagr_sop = df_vendas_krona[(df_vendas_krona["PERIODO"] >= primeiro_mes_vendas) & (df_vendas_krona["PERIODO"] <= ultimo_mes_vendas)].copy()

# Chaves para Desagregação do S&OP
chaves_desagr_sop = ["REGIONAL_GESTOR", "REGIONAL", "FAMILIA"]

# -----------------------------------------------------------------------------
# Desagregar plano S&OP com base no mix consolidado dos últimos 12 meses
# -----------------------------------------------------------------------------

# Nível agregado do plano S&OP
chaves_plano_sop = [
    "REGIONAL_GESTOR",
    "REGIONAL",
    "FAMILIA"
]

# Nível detalhado que deverá ser preservado na desagregação
chaves_detalhe_sop = [
    "EMPRESA",
    "COD_PROD",
    "DESC_PRODUTO",
    "FAMILIA",
    "LINHA",
    "REGIONAL",
    "REGIONAL_GESTOR",
    "SEGMENTO"
]

# Consolidar os últimos 12 meses no nível detalhado
df_base_participacao_sop = (
    df_vendas_krona_base_desagr_sop
    .groupby(
        chaves_detalhe_sop,
        as_index=False,
        dropna=False
    )["VOL_VENDA"]
    .sum()
)

# Calcular o total de venda por Regional Gestor + Regional + Família
df_base_participacao_sop["TOTAL_VOL_VENDA"] = (
    df_base_participacao_sop
    .groupby(
        chaves_plano_sop,
        dropna=False
    )["VOL_VENDA"]
    .transform("sum")
)

# Calcular participação de cada linha detalhada
df_base_participacao_sop["PARTIC_SOP"] = np.where(
    df_base_participacao_sop["TOTAL_VOL_VENDA"] > 0,
    (
        df_base_participacao_sop["VOL_VENDA"] /
        df_base_participacao_sop["TOTAL_VOL_VENDA"]
    ),
    0
)

# Cruzar plano agregado futuro com a participação histórica
df_plano_sop_desagregado = pd.merge(
    df_plano_sop,
    df_base_participacao_sop,
    on=chaves_plano_sop,
    how="left"
)

# Tratar eventuais combinações sem histórico
df_plano_sop_desagregado["PARTIC_SOP"] = (
    df_plano_sop_desagregado["PARTIC_SOP"]
    .fillna(0)
)

# Desagregar valor do plano
df_plano_sop_desagregado["VOL_VENDA_DESAGREGADO"] = (
    df_plano_sop_desagregado["VALOR"] *
    df_plano_sop_desagregado["PARTIC_SOP"]
)

# Eliminar colunas desnecessárias após a desagregação
colunas_eliminar = ["VALOR", "VOL_VENDA", "TOTAL_VOL_VENDA", "PARTIC_SOP"]
df_plano_sop_desagregado = df_plano_sop_desagregado.drop(columns=colunas_eliminar)

# Renomar coluna VOL_VENDA_DESAGREGADO para VOL_VENDA
df_plano_sop_desagregado.rename(columns={"VOL_VENDA_DESAGREGADO": "VOL_VENDA"}, inplace=True)

# Carregar DIM_PRODUTOS_KRONA para importar o PESO_UNIT
dim_produtos_krona = pd.read_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet")

# Importar o PESO_UNIT do DIM_PRODUTOS_KRONA para o df_plano_sop_desagregado
df_plano_sop_desagregado = pd.merge(
    df_plano_sop_desagregado,
    dim_produtos_krona[["COD_PROD", "PESO_UNIT"]],
    on="COD_PROD",
    how="left"
)

# Criar coluna QTD_VENDA a partir do VOL_VENDA e PESO_UNIT
df_plano_sop_desagregado["QTD_VENDA"] = np.where(
    df_plano_sop_desagregado["PESO_UNIT"] > 0,
    df_plano_sop_desagregado["VOL_VENDA"] / df_plano_sop_desagregado["PESO_UNIT"],
    0
)

# Eliminar coluna PESO_UNIT, pois não é mais necessária
df_plano_sop_desagregado = df_plano_sop_desagregado.drop(columns=["PESO_UNIT"])

# Organizar colunas no mesmo padrão da df_vendas_krona
colunas_ordenadas_final = [
    "EMPRESA",
    "COD_PROD",
    "DESC_PRODUTO",
    "FAMILIA",
    "LINHA",
    "REGIONAL",
    "REGIONAL_GESTOR",
    "SEGMENTO",
    "PERIODO",
    "QTD_VENDA",
    "VOL_VENDA"
]
df_plano_sop_desagregado = df_plano_sop_desagregado[colunas_ordenadas_final]

# Unificar df_vendas_krona com df_plano_sop_desagregado
df_hist_vend_sop = pd.concat([df_vendas_krona, df_plano_sop_desagregado], ignore_index=True)

# Agrupar dados para consolidar possíveis duplicações após a união
df_hist_vend_sop = (
    df_hist_vend_sop

    .groupby(
        [
            "EMPRESA",
            "COD_PROD",
            "DESC_PRODUTO",
            "FAMILIA",
            "LINHA",
            "REGIONAL",
            "REGIONAL_GESTOR",
            "SEGMENTO",
            "PERIODO"
        ],
        as_index=False
    )
    .agg(
        {
            "QTD_VENDA": "sum",
            "VOL_VENDA": "sum"
        }
    )
)

# Salvar df_hist_vend_sop em parquet para uso posterior
df_hist_vend_sop.to_parquet(pasta_staging_parquet / "df_hist_vend_sop.parquet", index=False)

del df_vendas_krona, df_plano_sop_desagregado, df_plano_sop, df_base_participacao_sop, df_vendas_krona_base_desagr_sop
gc.collect()

print("✅ Formação do histórico de vendas complementado com o S&OP, concluída!")

# %% [markdown]
# #### 09. PROCESSAMENTO DE MODELOS PARA PREVISAO ESTATISTICA

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
    # 0) CARREGAR df_hist_vend_sop DO PARQUET
    # ============================================================
    df_hist_vend_sop = pd.read_parquet(pasta_staging_parquet / "df_hist_vend_sop.parquet")
    print(f"📦 df_hist_vend_sop carregado | Linhas: {len(df_hist_vend_sop):,}")

    # ============================================================
    # FILTRO OPCIONAL PARA TESTE DE UM OU MAIS PRODUTOS
    # ============================================================
    if MODO_TESTE_COD_PROD:

        df_hist_vend_sop["COD_PROD"] = (
            df_hist_vend_sop["COD_PROD"]
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

        df_hist_vend_sop = df_hist_vend_sop[
            df_hist_vend_sop["COD_PROD"].isin(codigos_teste)
        ].copy()

        print(
            f"🧪 MODO TESTE ATIVO | Produtos base={codigos_base} | "
            f"Linhas após filtro: {len(df_hist_vend_sop):,}"
        )

        if df_hist_vend_sop.empty:
            raise ValueError(f"Nenhuma linha encontrada para COD_PROD em {codigos_base}")

    else:
        print("🏭 MODO COMPLETO ATIVO | Processando todos os produtos.")

    # ============================================================
    # 1) AGRUPAMENTO PADRÃO (COD_PROD + REGIONAL + PERIODO)
    # ============================================================
    df_group = (
        df_hist_vend_sop
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
    # 2) PERÍODO DE PREVISÃO ESTATÍSTICA - ORÇAMENTO
    # Sempre projetar o próximo ano completo
    # ============================================================
    ano_orcamento = pd.Timestamp.today().year + 1

    future_dates = pd.date_range(
        start=f"{ano_orcamento}-01-01",
        end=f"{ano_orcamento}-12-01",
        freq="MS"
    )

    horizon = len(future_dates)

    print(f"Ano do orçamento: {ano_orcamento}")
    print(f"Período previsto: {future_dates.min():%m/%Y} até {future_dates.max():%m/%Y}")
    print(f"Horizonte de previsão: {horizon} meses")

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

    df_forecast_estatistico_orcamento = pd.concat([df_final_hist, df_forecast], ignore_index=True)
    df_forecast_estatistico_orcamento = df_forecast_estatistico_orcamento.sort_values(
        ["COD_PROD", "REGIONAL", "PERIODO", "MODELO_ESCOLHIDO"],
        na_position="first"
    ).reset_index(drop=True)

    print("📦 Dataset base montado (histórico + futuro).")

    # ============================================================
    # 6.1) BACKTEST COMPLETO (walk-forward) com o MESMO modelo do futuro
    # ============================================================
    print("🧪 Iniciando backtest completo (walk-forward) por série...")

    STEP_BACKTEST = 6
    df_forecast_estatistico_orcamento["PREVISAO_BACKTEST"] = np.nan
    df_forecast_estatistico_orcamento["MODELO_BACKTEST"] = np.nan
    df_forecast_estatistico_orcamento["APE"] = np.nan
    df_forecast_estatistico_orcamento["MAPE_SKU"] = np.nan

    MIN_TREINO = 12

    df_hist_all = df_forecast_estatistico_orcamento[df_forecast_estatistico_orcamento["MODELO_ESCOLHIDO"].isna()].copy()
    df_hist_all = df_hist_all.sort_values(["COD_PROD", "REGIONAL", "PERIODO"])

    total_series_bt = df_hist_all[["COD_PROD","REGIONAL"]].drop_duplicates().shape[0]
    print(f"🔎 Backtest completo | Total séries: {total_series_bt:,} | MIN_TREINO={MIN_TREINO}")

    mask_hist = df_forecast_estatistico_orcamento["MODELO_ESCOLHIDO"].isna()

    idx_map_hist = (
        df_forecast_estatistico_orcamento.loc[mask_hist]
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
            df_forecast_estatistico_orcamento.loc[idx_rows, "PREVISAO_BACKTEST"] = preds
            df_forecast_estatistico_orcamento.loc[idx_rows, "MODELO_BACKTEST"] = best
            df_forecast_estatistico_orcamento.loc[idx_rows, "APE"] = ape
            df_forecast_estatistico_orcamento.loc[idx_rows, "MAPE_SKU"] = mape_serie

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
        df_forecast_estatistico_orcamento.set_index(["COD_PROD","REGIONAL"]).index.map(mape_por_serie_final),
        index=df_forecast_estatistico_orcamento.index,
        dtype=float
    )

    df_forecast_estatistico_orcamento["MAPE_SKU"] = df_forecast_estatistico_orcamento["MAPE_SKU"].fillna(mape_series)

    # ============================================================
    # LIMPAR BACKTEST NAS LINHAS FUTURAS
    # Backtest só deve existir no histórico.
    # Futuro deve manter apenas previsão final e MAPE_SKU da série.
    # ============================================================
    mask_futuro = df_forecast_estatistico_orcamento["PERIODO"].isin(future_dates)

    df_forecast_estatistico_orcamento.loc[
        mask_futuro,
        ["PREVISAO_BACKTEST", "MODELO_BACKTEST", "APE"]
    ] = np.nan


    # PREVISAO_FINAL: no futuro usa VOL_VENDA_REAL (que é o forecast),
    # no histórico usa PREVISAO_BACKTEST
    df_forecast_estatistico_orcamento["PREVISAO_FINAL"] = np.where(
        df_forecast_estatistico_orcamento["PERIODO"].isin(future_dates),
        df_forecast_estatistico_orcamento["VOL_VENDA_REAL"],
        df_forecast_estatistico_orcamento["PREVISAO_BACKTEST"]
    )

    print("✅ Backtest completo concluído (histórico preenchido).")

    # ============================================================
    # 7) SALVAR CSV
    # Se o arquivo padrão estiver aberto/bloqueado, salva com complemento no nome
    # ============================================================
    if MODO_TESTE_COD_PROD:
        arquivo_saida = pasta_staging_parquet / "df_forecast_estatistico_orcamento_TESTE.csv"
    else:
        arquivo_saida = pasta_staging_parquet / "df_forecast_estatistico_orcamento.csv"

    try:
        df_forecast_estatistico_orcamento.to_csv(
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

        df_forecast_estatistico_orcamento.to_csv(
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

# %% [markdown]
# #### 10. CONSOLIDACAO E DESAGREGACAO DA PREVISAO ESTATISTICA

# %%
# ============================================================
# DESAGREGAÇÃO DO FORECAST ESTATÍSTICO
# ============================================================

# Carregando Forecast Estatístico Krona para desagregação
arquivo_forecast = pasta_staging_parquet / "df_forecast_estatistico_orcamento.csv"

df_forecast_estatistico_orcamento = pd.read_csv(
    arquivo_forecast,
    sep=";",
    encoding="utf-8-sig",
    decimal=",",
    dtype={"COD_PROD": str},
    dayfirst=True
)

df_forecast_estatistico_orcamento["PERIODO"] = pd.to_datetime(df_forecast_estatistico_orcamento["PERIODO"])
df_forecast_estatistico_orcamento = df_forecast_estatistico_orcamento[df_forecast_estatistico_orcamento["PREVISAO_FINAL"].notna()].copy()

# Eliminar colunas desnecessárias no df_forecast_estatistico_orcamento
df_forecast_estatistico_orcamento = df_forecast_estatistico_orcamento.drop(
    columns=["MODELO_ESCOLHIDO", "PREVISAO_BACKTEST", "MODELO_BACKTEST", "APE", "MAPE_SKU", "VOL_VENDA_REAL"]
)

# Filtrar PERIODO considerando o próximo os meses do próximo ano (orçamento)
df_forecast_estatistico_orcamento = df_forecast_estatistico_orcamento[
    df_forecast_estatistico_orcamento["PERIODO"].dt.year == (pd.Timestamp.today().year + 1)
].copy()

# =========================
# DEFINIR HISTÓRICO PARA DESAGREGAÇÃO DO FORECAST
# =========================
meses_hist_desagregacao = 12

# Carregar df_vendas_krona do parquet para definir o histórico de vendas
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")
df_vendas_krona["PERIODO"] = pd.to_datetime(df_vendas_krona["PERIODO"])

# df_prev_krona deve ser cópia de df_vendas_krona, filtrando PERIODO pela variavel meses_hist_desagregacao, retornar os ultimos 12 que constam no arquivo df_vendas_krona
periodos_disponiveis = sorted(df_vendas_krona["PERIODO"].unique())
periodos_para_manter = periodos_disponiveis[-meses_hist_desagregacao:]
df_prev_krona = df_vendas_krona[df_vendas_krona["PERIODO"].isin(periodos_para_manter)].copy().reset_index(drop=True)

# Agrupar dados somando as VOL_VENDA
chaves_desagregacao = ["EMPRESA","COD_PROD","FAMILIA","LINHA","REGIONAL","REGIONAL_GESTOR","SEGMENTO"]

chaves_sem_periodo = chaves_desagregacao[:]
df_prev_krona = df_prev_krona.groupby(
    chaves_desagregacao,
    as_index=False
).agg({'VOL_VENDA': 'sum'}).reset_index(drop=True)

# Criar coluna TOTAL_VOL_VENDA por COD_PROD e REGIONAL
total_vol_venda_por_prod = df_prev_krona.groupby(['COD_PROD', 'REGIONAL'])['VOL_VENDA'].transform('sum')
df_prev_krona["TOTAL_VOL_VENDA"] = total_vol_venda_por_prod

# Criar coluna PERC_DESAGR
df_prev_krona["PERC_DESAGR"] = df_prev_krona["VOL_VENDA"] / df_prev_krona["TOTAL_VOL_VENDA"]

df_prev_explodido = (
    df_prev_krona
    .merge(
        df_forecast_estatistico_orcamento,
        on=["COD_PROD", "REGIONAL"],
        how="inner"   # só explode onde existe forecast
    )
)

df_prev_explodido["VOL_PREV"] = (df_prev_explodido["PREVISAO_FINAL"] * df_prev_explodido["PERC_DESAGR"])
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
colunas_finais = ["EMPRESA", "COD_PROD", "FAMILIA", "LINHA", "REGIONAL", "REGIONAL_GESTOR", "SEGMENTO", "PERIODO", "VOL_PREV", "QTD_PREV"]

# Agrupar Dados finais por chaves e somar VOL_PREV e QTD_PREV
df_prev_krona = (
    df_prev_krona[colunas_finais]
    .groupby(
        ["EMPRESA", "COD_PROD", "FAMILIA", "LINHA", "REGIONAL", "REGIONAL_GESTOR", "SEGMENTO", "PERIODO"],
        as_index=False
    )
    .agg({'VOL_PREV': 'sum', 'QTD_PREV': 'sum'})
    .reset_index(drop=True)
)

# Salvar df_prev_krona em parquet para uso posterior
df_prev_krona.to_parquet(
    pasta_staging_parquet / "df_prev_krona_orcamento.parquet",
    engine="pyarrow",
    compression="snappy",
    index=False
)

# FIXME
del df_prev_explodido, df_forecast_estatistico_orcamento, df_vendas_krona, df_dim_peso_unit_vendas, total_vol_venda_por_prod, periodos_disponiveis, periodos_para_manter, chaves_desagregacao, chaves_sem_periodo, colunas_finais, df_prev_krona
gc.collect()

# %% [markdown]
# #### 11. GERAR BASES PARA PAINEL ORCAMENTO_KRONA

# %%
# -------------------------------------------------------------------------
# GERAR BASES PARA ALIMENTAR PAINEL DE PLANEJAMENTO DO ORCAMENTO
# -----------------------------------------------------------------------

# Carregar df_hist_vend_sop do parquet para gerar histórico de vendas
df_hist_vend_sop = pd.read_parquet(pasta_staging_parquet / "df_hist_vend_sop.parquet")
df_hist_vend_sop["PERIODO"] = pd.to_datetime(df_hist_vend_sop["PERIODO"])

# Colunas para agrupar e somar
colunas_agrupamento = ["FAMILIA", "REGIONAL", "REGIONAL_GESTOR", "SEGMENTO", "PERIODO"]

# Agrupar filtrando o Ano atual
df_hist_vend_ANO_ATUAL = (
    df_hist_vend_sop[df_hist_vend_sop["PERIODO"].dt.year == pd.Timestamp.today().year]
    .groupby(colunas_agrupamento, as_index=False)
    .agg({'VOL_VENDA': 'sum'})
    .reset_index(drop=True)
)

# Criar coluna ID, concatenando REGIONAL, FAMILIA, SEGMENTO e PERIODO, e PERIODO no formato MMMYY
df_hist_vend_ANO_ATUAL["ID"] = (
    df_hist_vend_ANO_ATUAL["REGIONAL"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL["FAMILIA"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL["SEGMENTO"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL["PERIODO"].dt.strftime("%b%y").str.upper()
)

# Agrupar filtrando o Ano anterior (Ano atual - 1)
df_hist_vend_ANO_ATUAL_MENOS_1 = (
    df_hist_vend_sop[df_hist_vend_sop["PERIODO"].dt.year == (pd.Timestamp.today().year - 1)]
    .groupby(colunas_agrupamento, as_index=False)
    .agg({'VOL_VENDA': 'sum'})
    .reset_index(drop=True)
)

# Criar coluna ID, concatenando REGIONAL, FAMILIA, SEGMENTO e PERIODO, e PERIODO no formato MMMYY
df_hist_vend_ANO_ATUAL_MENOS_1["ID"] = (
    df_hist_vend_ANO_ATUAL_MENOS_1["REGIONAL"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL_MENOS_1["FAMILIA"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL_MENOS_1["SEGMENTO"].astype(str) + "|" +
    df_hist_vend_ANO_ATUAL_MENOS_1["PERIODO"].dt.strftime("%b%y").str.upper()
)

# Salvando os arquivos em CSV para alimentar o painel de planejamento do orçamento
df_hist_vend_ANO_ATUAL.to_csv(
    pasta_input_painel / 'ORC_HIST_VEND_ANO_ATUAL.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

df_hist_vend_ANO_ATUAL_MENOS_1.to_csv(
    pasta_input_painel / 'ORC_HIST_VEND_ANO_ANT.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

# Carregar df_prev_krona do parquet
df_prev_krona = pd.read_parquet(pasta_staging_parquet / "df_prev_krona_orcamento.parquet")

# Agrupar valores
df_prev_krona_agrupado = (
    df_prev_krona.groupby(colunas_agrupamento, as_index=False)
    .agg({'VOL_PREV': 'sum'})
    .reset_index(drop=True)
)

# Criar coluna ID, concatenando REGIONAL, FAMILIA, SEGMENTO e PERIODO, e PERIODO no formato MMMYY maisculo
df_prev_krona_agrupado["ID"] = (
    df_prev_krona_agrupado["REGIONAL"].astype(str) + "|" +
    df_prev_krona_agrupado["FAMILIA"].astype(str) + "|" +
    df_prev_krona_agrupado["SEGMENTO"].astype(str) + "|" +
    df_prev_krona_agrupado["PERIODO"].dt.strftime("%b%y").str.upper()
)

# Salvar o arquivo em CSV para alimentar o painel de planejamento do orçamento
df_prev_krona_agrupado.to_csv(
    pasta_input_painel / 'ORC_PREV_KRONA.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

# Carregar df_vendas_krona do parquet
df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")

# Filtrar os ultimos 12 meses de vendas
df_vendas_krona_ultimos_12_meses = (
    df_vendas_krona[df_vendas_krona["PERIODO"] >= (pd.Timestamp.today() - pd.DateOffset(months=12))]
    .copy()
)

# Agrupar valores retornando a soma de VOL_VENDA e VAL_VENDA dos últimos 12 meses por FAMILIA, REGIONAL, REGIONAL_GESTOR, SEGMENTO
df_vendas_krona_ultimos_12_meses_agrupado = (
    df_vendas_krona_ultimos_12_meses.groupby(colunas_agrupamento, as_index=False)
    .agg({'VOL_VENDA': 'sum', 'VAL_VENDA': 'sum'})
    .reset_index(drop=True)
)

# Gerando Média dos ultimos 12 meses
colunas_media_12_meses = ["FAMILIA", "REGIONAL", "REGIONAL_GESTOR", "SEGMENTO"]

df_vendas_krona_media_12_meses = (
    df_vendas_krona_ultimos_12_meses_agrupado
    .groupby(
        colunas_media_12_meses,
        as_index=False
    )
    .agg(
        MEDIA_VOL_VENDA_12M=("VOL_VENDA", "mean"),
        MEDIA_VAL_VENDA_12M=("VAL_VENDA", "mean")
    )
)

# Criar Coluna RS_KG
df_vendas_krona_media_12_meses["RS_KG"] = (
    df_vendas_krona_media_12_meses["MEDIA_VAL_VENDA_12M"] / df_vendas_krona_media_12_meses["MEDIA_VOL_VENDA_12M"]
)

# Criar coluna ID, concatenando REGIONAL, FAMILIA, SEGMENTO e PERIODO
df_vendas_krona_media_12_meses["ID"] = (
    df_vendas_krona_media_12_meses["REGIONAL"].astype(str) + "|" +
    df_vendas_krona_media_12_meses["FAMILIA"].astype(str) + "|" +
    df_vendas_krona_media_12_meses["SEGMENTO"].astype(str)
)

# Salvar em xlsx para Karol utilizar caso alguém questione os valores de média dos últimos 12 meses
df_vendas_krona_media_12_meses.to_excel(
    pasta_input_painel / 'ORC_VEND_MEDIA_12_MESES.xlsx',
    index=False,
    float_format="%.2f"
)

# Salvar em CSV para alimentar o painel de planejamento do orçamento
df_vendas_krona_media_12_meses.to_csv(
    pasta_input_painel / 'ORC_HIST_RS_KG_12_MESES.csv',
    sep=';',
    encoding='utf-8-sig',
    index=False,
    decimal=',',
    float_format="%.2f"
)

del df_hist_vend_ANO_ATUAL, df_hist_vend_ANO_ATUAL_MENOS_1, df_hist_vend_sop, df_prev_krona, df_prev_krona_agrupado
gc.collect()
print("🎯 Bases para Painel de Orçamento, geradas com sucesso!")

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


