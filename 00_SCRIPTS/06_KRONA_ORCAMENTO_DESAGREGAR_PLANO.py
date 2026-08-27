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
# #### 02. CARREGANDO DADOS DO PAINEL ORCAMENTO_KRONA

# %%
# Importando o plano orcamento

origem = pasta_painel / 'ORCAMENTO_KRONA.xlsb'
copia = pasta_painel / 'ORCAMENTO_KRONA_TEMP.xlsb'

if copia.exists():
    copia.unlink()

shutil.copy2(origem, copia)

# Carregar o arquivo Excel, a partir da 4ª linha
df_plano_orc= pd.read_excel(copia, sheet_name='PLAN_ORCAMENTO', engine='pyxlsb', skiprows=6)

# ============================================================
# TRANSFORMANDO O PLANO ORÇAMENTÁRIO PARA FORMATO LONG
#    - Os rótulos reais estão na linha 0 do DataFrame
#    - Mantém ID, REGIONAL GESTOR, REGIONAL, SEGMENTO e FAMILIA
#    - Transforma Estatístico e Consenso em linhas
#    - Cria METRICA, PERIODO e VALOR
# ============================================================

colunas_id = ["ID", "REGIONAL GESTOR", "REGIONAL", "SEGMENTO", "FAMILIA"]

# Lê os rótulos da linha 0 sem alterar os nomes físicos das colunas
rotulos = df_plano_orc.iloc[0].astype(str).str.strip()

# ============================================================
# EXCLUIR COLUNAS DE TOTAL ANTES DA TRANSPOSIÇÃO
#    - Verifica os nomes físicos das colunas
#    - Remove qualquer coluna cujo nome contenha TOTAL
# ============================================================

colunas_total = [
    coluna for coluna in df_plano_orc.columns
    if "TOTAL" in str(coluna).upper()
]

df_plano_orc = df_plano_orc.drop(columns=colunas_total)

# Recalcula os rótulos após excluir as colunas de TOTAL
rotulos = df_plano_orc.iloc[0].astype(str).str.strip()

# ============================================================
# LOCALIZAR AS COLUNAS FÍSICAS DAS DIMENSÕES
# ============================================================

map_colunas_id = {
    rotulo: coluna
    for coluna, rotulo in zip(df_plano_orc.columns, rotulos)
    if rotulo in colunas_id
}

# Colunas físicas usadas como identificadores no melt
id_vars = list(map_colunas_id.values())

# ============================================================
# LOCALIZAR AS COLUNAS DE ESTATÍSTICO E CONSENSO
# ============================================================

colunas_metricas = [
    coluna
    for coluna, rotulo in zip(df_plano_orc.columns, rotulos)
    if "Estatístico" in rotulo or "Consenso" in rotulo
]

# ============================================================
# CRIAR O DE-PARA DAS MÉTRICAS
#    Exemplo:
#    Estatístico [KG] -> ESTATISTICO_KG
#    Estatístico [R$] -> ESTATISTICO_R$
#    Consenso [KG]    -> CONSENSO_KG
#    Consenso [R$]    -> CONSENSO_R$
# ============================================================

map_metrica = {}

for coluna, rotulo in zip(df_plano_orc.columns, rotulos):
    rotulo_up = rotulo.upper()

    if "ESTATÍSTICO" in rotulo_up:
        metrica = "ESTATISTICO"
    elif "CONSENSO" in rotulo_up:
        metrica = "CONSENSO"
    else:
        continue

    if "KG" in rotulo_up:
        metrica += "_KG"
    elif "R$" in rotulo_up:
        metrica += "_R$"

    map_metrica[coluna] = metrica

# ============================================================
# REMOVER A LINHA AUXILIAR DOS RÓTULOS
# ============================================================

df_plano_orc_long = df_plano_orc.iloc[1:].copy()

# ============================================================
# TRANSFORMAR COLUNAS MENSAIS EM LINHAS
# ============================================================

df_plano_orc_long = df_plano_orc_long.melt(
    id_vars=id_vars,
    value_vars=colunas_metricas,
    var_name="PERIODO",
    value_name="VALOR"
)

# ============================================================
# RENOMEAR AS COLUNAS DE IDENTIFICAÇÃO
# ============================================================

# Renomear colunas de identificação e padronizar REGIONAL_GESTOR
df_plano_orc_long = df_plano_orc_long.rename(
    columns={
        coluna: ("REGIONAL_GESTOR" if rotulo == "REGIONAL GESTOR" else rotulo)
        for rotulo, coluna in map_colunas_id.items()
    }
)

# ============================================================
# CRIAR A MÉTRICA
# ============================================================

df_plano_orc_long["METRICA"] = df_plano_orc_long["PERIODO"].map(map_metrica)

# ============================================================
# TRATAR O PERÍODO
#    O pandas cria .1, .2 etc. quando existem colunas
#    repetidas para a mesma data
# ============================================================

df_plano_orc_long["PERIODO"] = (
    df_plano_orc_long["PERIODO"]
    .astype(str)
    .str.replace(r"\.\d+$", "", regex=True)
)

# Converter serial Excel para data
df_plano_orc_long["PERIODO"] = pd.to_datetime(
    pd.to_numeric(df_plano_orc_long["PERIODO"], errors="coerce"),
    unit="D",
    origin="1899-12-30"
)

# ============================================================
# CRIAR COLUNA ANO COM BASE NO PERÍODO
# ============================================================

df_plano_orc_long["ANO"] = df_plano_orc_long["PERIODO"].dt.year

# ============================================================
# ORGANIZAR COLUNAS FINAIS
# ============================================================

df_plano_orc_long = df_plano_orc_long[
    ["ID", "REGIONAL_GESTOR", "REGIONAL", "SEGMENTO", "FAMILIA", "METRICA", "ANO", "PERIODO", "VALOR"]
]

# ============================================================
# EXCLUIR ARQUIVO TEMPORÁRIO
# ============================================================

if copia.exists():
    copia.unlink()
    
print("✅ Dados consolidados do painel, carregados com sucesso!")

# %% [markdown]
# #### 03. SALVANDO PLANO NO BD E CRIANDO VERSAO DE DADOS

# %%
# ============================================================
# VERSIONAR E SALVAR O PLANO ORÇAMENTÁRIO CONSOLIDADO
#    - Cria o banco Parquet caso ainda não exista
#    - Mantém o histórico das versões anteriores
#    - Versiona separadamente cada ANO de orçamento
#    - Registra data/hora da gravação
# ============================================================

arquivo_plano = pasta_hist_planos / "BD_PLANO_ORCAMENTO_CONSOLIDADO_KRONA.parquet"

# ============================================================
# CARREGAR BANCO EXISTENTE
# ============================================================

if arquivo_plano.exists():
    df_banco_plano_consolidado = pd.read_parquet(arquivo_plano)
else:
    df_banco_plano_consolidado = pd.DataFrame()

# ============================================================
# PREPARAR O PLANO ATUAL
# ============================================================

df_plano_salvar = df_plano_orc_long.copy()

# Identificar os anos existentes no plano atual
anos_plano = df_plano_salvar["ANO"].dropna().unique()

# ============================================================
# DEFINIR A VERSÃO DE CADA ANO
# ============================================================

map_versao = {}

for ano in anos_plano:
    if not df_banco_plano_consolidado.empty:
        versoes_ano = df_banco_plano_consolidado.loc[
            df_banco_plano_consolidado["ANO"] == ano,
            "VERSAO"
        ]

        nova_versao = int(versoes_ano.max()) + 1 if not versoes_ano.empty else 1
    else:
        nova_versao = 1

    map_versao[ano] = nova_versao

# Aplicar a versão correspondente a cada ano
df_plano_salvar["VERSAO"] = df_plano_salvar["ANO"].map(map_versao)

# Registrar data/hora da gravação
df_plano_salvar["DATA_VERSAO"] = pd.Timestamp.now()

# ============================================================
# ORGANIZAR COLUNAS
# ============================================================

df_plano_salvar = df_plano_salvar[
    ["VERSAO", "DATA_VERSAO", "ID", "REGIONAL_GESTOR", "REGIONAL",
     "SEGMENTO", "FAMILIA", "METRICA", "PERIODO", "ANO", "VALOR"]
]

# ============================================================
# CONCATENAR COM O HISTÓRICO
# ============================================================

if df_banco_plano_consolidado.empty:
    df_banco_plano_consolidado = df_plano_salvar.copy()
else:
    df_banco_plano_consolidado = pd.concat(
        [df_banco_plano_consolidado, df_plano_salvar],
        ignore_index=True
    )

# ============================================================
# SALVAR BANCO ATUALIZADO
# ============================================================

df_banco_plano_consolidado.to_parquet(arquivo_plano, index=False)

print("✅ Plano consolidado salvo com sucesso!")
print(f"Versões gravadas: { {int(ano): int(versao) for ano, versao in map_versao.items()} }")

# %% [markdown]
# #### 04. DESAGREGAR PLANO CONSOLIDADO E GERAR PLANO DETALHADO

# %%



# Carregar histórico de vendas
df_vendas_krona = pd.read_parquet(
    pasta_staging_parquet / "df_vendas_krona.parquet"
)

# Filtrar últimos 12 meses para base de desagregação
ultimo_mes_vendas = df_vendas_krona["PERIODO"].max()
primeiro_mes_vendas = ultimo_mes_vendas - pd.DateOffset(months=11)

df_vendas_krona_base_desagr_plan_orc = df_vendas_krona[
    (df_vendas_krona["PERIODO"] >= primeiro_mes_vendas) &
    (df_vendas_krona["PERIODO"] <= ultimo_mes_vendas)
].copy()

# Carregando plano que deverá ser desagregado para o nível detalhado
df_plano_orc_para_desagr = df_plano_orc_long.copy()

# Filtrar somente METRICA = CONSENSO_KG, pois o plano orçamentário será desagregado para o nível detalhado de produto
df_plano_orc_para_desagr = df_plano_orc_para_desagr[
    df_plano_orc_para_desagr["METRICA"] == "CONSENSO_KG"
]

# Chaves para Desagregação
chaves_desagr_plan_orc = ["REGIONAL_GESTOR", "REGIONAL", "SEGMENTO", "FAMILIA"]
chaves_detalhe_plan_orc = ["EMPRESA", "COD_PROD", "DESC_PRODUTO", "FAMILIA", "LINHA", "REGIONAL", "REGIONAL_GESTOR", "SEGMENTO"]

# Consolidar os últimos 12 meses no nível detalhado
df_base_participacao_plan_orc = (
    df_vendas_krona_base_desagr_plan_orc
    .groupby(chaves_detalhe_plan_orc, as_index=False, dropna=False)["VOL_VENDA"]
    .sum()
)

# Calcular total de venda por Regional Gestor + Regional + Segmento + Família
df_base_participacao_plan_orc["TOTAL_VOL_VENDA"] = (
    df_base_participacao_plan_orc
    .groupby(chaves_desagr_plan_orc, dropna=False)["VOL_VENDA"]
    .transform("sum")
)

# Calcular participação de cada linha detalhada
df_base_participacao_plan_orc["PARTIC_PLAN_ORC"] = np.where(
    df_base_participacao_plan_orc["TOTAL_VOL_VENDA"] > 0,
    df_base_participacao_plan_orc["VOL_VENDA"] / df_base_participacao_plan_orc["TOTAL_VOL_VENDA"],
    0
)

# Cruzar plano orçamentário com a participação histórica
df_plano_orc_desagregado = pd.merge(
    df_plano_orc_para_desagr,
    df_base_participacao_plan_orc,
    on=chaves_desagr_plan_orc,
    how="left"
)

# Tratar combinações sem histórico
df_plano_orc_desagregado["PARTIC_PLAN_ORC"] = df_plano_orc_desagregado["PARTIC_PLAN_ORC"].fillna(0)

# Desagregar valor do plano
df_plano_orc_desagregado["VALOR_DESAGREGADO"] = (
    df_plano_orc_desagregado["VALOR"] *
    df_plano_orc_desagregado["PARTIC_PLAN_ORC"]
)

# Eliminar colunas auxiliares da desagregação
colunas_eliminar = ["VALOR", "VOL_VENDA", "TOTAL_VOL_VENDA", "PARTIC_PLAN_ORC", "ANO", "ID", "METRICA"]
df_plano_orc_desagregado = df_plano_orc_desagregado.drop(columns=colunas_eliminar)

# Renomear valor desagregado para VALOR
df_plano_orc_desagregado.rename(columns={"VALOR_DESAGREGADO": "VOL_ORC"}, inplace=True)

# Carregar arquivo para buscar preço médio e calcular o valor em R$, carregando o mesmo arquivo CSV que alimenta o painel ORCAMENTO_KRONA
df_preco_medio = pd.read_csv(
    pasta_input_painel / "ORC_HIST_RS_KG_12_MESES.csv",
    sep=";",
    decimal=","
)

# Importar preço médio por KG para o plano desagregado
df_plano_orc_desagregado = pd.merge(
    df_plano_orc_desagregado,
    df_preco_medio[chaves_desagr_plan_orc + ["RS_KG"]],
    on=chaves_desagr_plan_orc,
    how="left"
)

# Calcular valor orçado em R$
df_plano_orc_desagregado["VAL_ORC"] = (
    df_plano_orc_desagregado["VOL_ORC"] *
    df_plano_orc_desagregado["RS_KG"]
)

# Eliminar coluna auxiliar de preço médio
df_plano_orc_desagregado = df_plano_orc_desagregado.drop(columns=["RS_KG"])

# Organizar colunas finais
colunas_ordenadas_plan_orc = ["EMPRESA", "COD_PROD", "DESC_PRODUTO", "FAMILIA", "LINHA", "REGIONAL_GESTOR", "REGIONAL", "SEGMENTO", "PERIODO", "VOL_ORC", "VAL_ORC"]
df_plano_orc_desagregado = df_plano_orc_desagregado[colunas_ordenadas_plan_orc]

# Carregar DIM_PRODUTOS_KRONA.parquet para buscar peso unitário e calcular quantidade
df_dim_produtos = pd.read_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet")

# Buscar PESO_UNIT pelo campo COD_PROD
df_plano_orc_desagregado = pd.merge(
    df_plano_orc_desagregado,
    df_dim_produtos[["COD_PROD", "PESO_UNIT"]],
    on="COD_PROD",
    how="left"
)

# Criar coluna de quantidade orçada evitando divisão por zero
df_plano_orc_desagregado["PESO_UNIT"] = pd.to_numeric(df_plano_orc_desagregado["PESO_UNIT"], errors="coerce")
df_plano_orc_desagregado["QTD_ORC"] = 0.0

mask_peso = df_plano_orc_desagregado["PESO_UNIT"] > 0
df_plano_orc_desagregado.loc[mask_peso, "QTD_ORC"] = (
    df_plano_orc_desagregado.loc[mask_peso, "VOL_ORC"] /
    df_plano_orc_desagregado.loc[mask_peso, "PESO_UNIT"]
)

# Eliminar coluna auxiliar de peso unitário
df_plano_orc_desagregado = df_plano_orc_desagregado.drop(columns=["PESO_UNIT"])

# %% [markdown]
# #### 05. SALVAR O PLANO DETALHADO COM VERSIONAMENTO

# %%
# Salvar plano detalhado com controle de versão
arquivo_plano_detalhado = pasta_hist_planos / "BD_PLANO_ORCAMENTO_DETALHADO_KRONA.parquet"

# Criar ANO para controle de versão
df_plano_orc_desagregado["ANO"] = df_plano_orc_desagregado["PERIODO"].dt.year

# Se o parquet já existir, identificar a última versão por ANO e incrementar
if arquivo_plano_detalhado.exists():
    df_hist = pd.read_parquet(arquivo_plano_detalhado)

    ultima_versao = df_hist.groupby("ANO")["VERSAO"].max()

    df_plano_orc_desagregado["VERSAO"] = (
        df_plano_orc_desagregado["ANO"].map(ultima_versao).fillna(0).astype(int) + 1
    )

    # Registrar data/hora da nova versão
    df_plano_orc_desagregado["DATA_VERSAO"] = pd.Timestamp.now()

    # Adicionar nova versão ao histórico existente
    df_hist = pd.concat([df_hist, df_plano_orc_desagregado], ignore_index=True)

# Se não existir, criar primeira versão
else:
    df_plano_orc_desagregado["VERSAO"] = 1
    df_plano_orc_desagregado["DATA_VERSAO"] = pd.Timestamp.now()
    df_hist = df_plano_orc_desagregado.copy()

# Salvar histórico completo
df_hist.to_parquet(arquivo_plano_detalhado, index=False)

print("✅ Plano detalhado desagregado e salvo com sucesso!")

# %%
# Ativar ou desativar comparativo entre consolidado e detalhado
ATIVAR_COMPARATIVO_ORC = False

if ATIVAR_COMPARATIVO_ORC:

    # Consolidado - Volume
    df_comp_vol_consolidado = (
        df_plano_orc_long[df_plano_orc_long["METRICA"] == "CONSENSO_KG"]
        .groupby("PERIODO", as_index=False)["VALOR"]
        .sum()
        .rename(columns={"VALOR": "VOL_CONSOLIDADO"})
    )

    # Consolidado - Valor
    df_comp_val_consolidado = (
        df_plano_orc_long[df_plano_orc_long["METRICA"] == "CONSENSO_R$"]
        .groupby("PERIODO", as_index=False)["VALOR"]
        .sum()
        .rename(columns={"VALOR": "VAL_CONSOLIDADO"})
    )

    # Detalhado - Volume e Valor
    df_comp_detalhado = (
        df_plano_orc_desagregado
        .groupby("PERIODO", as_index=False)[["VOL_ORC", "VAL_ORC"]]
        .sum()
        .rename(columns={"VOL_ORC": "VOL_DETALHADO", "VAL_ORC": "VAL_DETALHADO"})
    )

    # Juntar comparativos
    df_comparativo_orc = (
        df_comp_vol_consolidado
        .merge(df_comp_val_consolidado, on="PERIODO", how="outer")
        .merge(df_comp_detalhado, on="PERIODO", how="outer")
    )

    # Calcular diferenças
    df_comparativo_orc["DIF_VOL"] = df_comparativo_orc["VOL_DETALHADO"] - df_comparativo_orc["VOL_CONSOLIDADO"]
    df_comparativo_orc["DIF_VAL"] = df_comparativo_orc["VAL_DETALHADO"] - df_comparativo_orc["VAL_CONSOLIDADO"]

    df_comparativo_orc["DIF_VOL_PCT"] = np.where(
        df_comparativo_orc["VOL_CONSOLIDADO"] != 0,
        df_comparativo_orc["DIF_VOL"] / df_comparativo_orc["VOL_CONSOLIDADO"] * 100,
        0
    )

    df_comparativo_orc["DIF_VAL_PCT"] = np.where(
        df_comparativo_orc["VAL_CONSOLIDADO"] != 0,
        df_comparativo_orc["DIF_VAL"] / df_comparativo_orc["VAL_CONSOLIDADO"] * 100,
        0
    )

    df_comparativo_orc = df_comparativo_orc.sort_values("PERIODO")

    display(df_comparativo_orc)

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


