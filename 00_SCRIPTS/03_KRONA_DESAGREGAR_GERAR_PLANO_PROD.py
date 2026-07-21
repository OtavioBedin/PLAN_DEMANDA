# %%
# Importando bibliotecas
from functions import *
import pandas as pd
import locale
from pathlib import Path
import shutil
from datetime import datetime
import warnings
import logging
from openpyxl import load_workbook

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
pasta_staging_parquet = caminho_base.parent / '02_STAGING_PARQUET'
pasta_painel = caminho_base.parent / '05_PAINEL'
pasta_historico_planos = caminho_base.parent / '04_HISTORICO_PLANOS'

print("✅ Mapeamento de pastas concluído com sucesso!")

# %%
# FIXME: ATENÇÂO DEV:
# Processo criado apenas para agregado por Regional, para acelerar o processo de enviar de dados para o Gabriel e Karolina
# Verificar com Alex e Karol, o ponto de planejamento por cliente se irá continuar, para desenvolver essa parte

# %%
# Carregando os planos agregados do painel, para pegar o ciclo mais recente, e a revisão mais recente, para desagregar

# Ler parquet plano agregado regional
df_plano_consenso_regional = pd.read_parquet(pasta_historico_planos / 'BD_PLANO_AGREGADO_PAINEL_REGIONAL.parquet')

# Retorna ultimo valor da coluna CICLO, para pegar o ciclo mais recente, classificando o PERIODO e a REVISAO
ultimo_ciclo= df_plano_consenso_regional.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['CICLO']
ultima_revisao = df_plano_consenso_regional.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['REVISAO']

# Filtrar os planos agregados para o ciclo mais recente e revisão mais recente
df_plano_consenso_regional = df_plano_consenso_regional[(df_plano_consenso_regional['CICLO'] == ultimo_ciclo) & (df_plano_consenso_regional['REVISAO'] == ultima_revisao)]

# Ler arquivo parquet de historico para desagregação
df_forecast_vendas_krona_PRODUTO = pd.read_parquet(pasta_staging_parquet / 'df_forecast_vendas_krona_PRODUTO.parquet')

# Padronizar PERIODO como datetime
df_plano_consenso_regional['PERIODO'] = pd.to_datetime(df_plano_consenso_regional['PERIODO'])

# Atualizar DESC_PROD, FAMILIA e LINHA para evitar a duplicidade conforme aconteceu com o COD_PROD 0088
df_dim_produtos = pd.read_parquet(pasta_staging_parquet / 'DIM_PRODUTOS_KRONA.parquet')

dim_idx = df_dim_produtos.set_index('COD_PROD')

map_cols = {
    'DESC_PROD': 'DESC_PRODUTO',
    'FAMILIA': 'FAMILIA',
    'LINHA': 'LINHA'
}

for col_dim, col_df in map_cols.items():
    novo = df_forecast_vendas_krona_PRODUTO['COD_PROD'].map(dim_idx[col_dim])

    if col_df in df_forecast_vendas_krona_PRODUTO.columns:
        df_forecast_vendas_krona_PRODUTO[col_df] = novo.fillna(
            df_forecast_vendas_krona_PRODUTO[col_df]
        )
    else:
        df_forecast_vendas_krona_PRODUTO[col_df] = novo


# Agrupar dados df_plano_consenso_regional somando os valores de DEMANDA_PLANEJADA
colunas_agrupamento = ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO', 'CICLO']
df_plano_consenso_regional_grouped = df_plano_consenso_regional.groupby(colunas_agrupamento)['VALOR'].sum().reset_index()

ciclo_plano =  df_plano_consenso_regional_grouped['CICLO'].iloc[0]

print("✅ Arquivos importados e preparados com sucesso!")

# %%
# 📥 Desagregação do plano REGIONAL

# Calculando a participação da cada linha de produto no volume total da combinação de chaves - REGIONAL_GESTOR, REGIONAL, FAMILIA, PERIODO
df_volume_desag_regional = df_forecast_vendas_krona_PRODUTO.copy()

chaves = ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO']

# total por combinação
df_volume_desag_regional['TOTAL'] = df_volume_desag_regional.groupby(chaves)['VOL_PREV'].transform('sum')

# participação da linha dentro da combinação
df_volume_desag_regional['PARTIC'] = np.where(
    df_volume_desag_regional['TOTAL'] > 0,
    df_volume_desag_regional['VOL_PREV'] / df_volume_desag_regional['TOTAL'],
    0
)

# Mesclar participação com o plano consenso regional unificado
df_volume_desag_regional = pd.merge(
    df_volume_desag_regional,
    df_plano_consenso_regional_grouped,
    on=chaves,
    how='left'
)

# Renomear colunas VALOR E VOL_PREV para evitar conflitos
df_volume_desag_regional.rename(columns={'VOL_PREV': 'VOL_ESTATISTICO'}, inplace=True)
df_volume_desag_regional.rename(columns={'VALOR': 'VOL_CONSENSO'}, inplace=True)

# Calcular desagregação
df_volume_desag_regional['VOL_CONSENSO_DESAGREGADO'] = df_volume_desag_regional['PARTIC'] * df_volume_desag_regional['VOL_CONSENSO']

# %%
# Transformar plano desagregado e peças e criar  o formato final, incluindo as colunas necessárias
df_plano_final_krona = df_volume_desag_regional.copy()

# Carregar parquet Dim_Produtos_Vendas_krona para buscar peso unitário
df_dim_produtos = pd.read_parquet(pasta_staging_parquet / 'DIM_PRODUTOS_KRONA.parquet')

# Mesclar peso unitário com o plano final
df_plano_final_krona = pd.merge(
    df_plano_final_krona,
    df_dim_produtos[['COD_PROD', 'PESO_UNIT']],
    on=['COD_PROD'],
    how='left'
)

df_plano_final_krona['QTD_CONSENSO'] = df_plano_final_krona['VOL_CONSENSO_DESAGREGADO'] / df_plano_final_krona['PESO_UNIT']
df_plano_final_krona['QTD_ESTATISTICO'] = df_plano_final_krona['VOL_ESTATISTICO'] / df_plano_final_krona['PESO_UNIT']

# Adicionar coluna de versão do plano
df_plano_final_krona['CICLO'] = ciclo_plano

# %%
# Gerar saída previsão de vendas em excel, com colunas específicas para arquivo do Gabriel

# Agrupar valores VOL_CONSENSO_DESAGREGADO e QTD_CONSENSO por coluna
colunas_grupo = ['COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'PERIODO']
colunas_valor = ['VOL_CONSENSO_DESAGREGADO', 'QTD_CONSENSO']
df_plano_saida_gabriel = df_plano_final_krona.groupby(colunas_grupo, as_index=False)[colunas_valor].sum()

# Renomear colunas
df_plano_saida_gabriel.rename(columns={'VOL_CONSENSO_DESAGREGADO': 'VOL_CONSENSO'}, inplace=True)

colunas_saida = ['COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'FAMILIA', 'LINHA', 'PERIODO', 'VOL_CONSENSO', 'QTD_CONSENSO']
df_plano_saida_gabriel = df_plano_saida_gabriel[colunas_saida]

# Apagar o arquivo antigo da pasta de staging caso exista, que tenha no nome plano_saida_gabriel_
for arquivo in pasta_staging_parquet.glob(f'plano_saida_gabriel_*.xlsx'):
    arquivo.unlink()

# # Salvar arquivo Excel
caminho_saida_excel = pasta_staging_parquet / f'plano_saida_gabriel_{ciclo_plano}.xlsx'
df_plano_saida_gabriel.to_excel(caminho_saida_excel, index=False)

# %%
# Gerar saída de novos produtos para arquivo do Gabriel 

# Importar demanda de lancamentos salva e resolvida em parquet
df_demanda_produtos_lancamento = pd.read_parquet(pasta_staging_parquet / 'df_demanda_produtos_lancamento.parquet')

# Apagar o arquivo antigo da pasta de staging caso exista, que tenha no nome plano_saida_gabriel_lancamentos_
for arquivo in pasta_staging_parquet.glob(f'plano_saida_gabriel_lancamentos_*.xlsx'):
    arquivo.unlink()

# Salvar arquivo Excel
caminho_saida_excel_gabriel_lancamentos = pasta_staging_parquet / f'plano_saida_gabriel_lancamentos_{ciclo_plano}.xlsx'
df_demanda_produtos_lancamento.to_excel(caminho_saida_excel_gabriel_lancamentos, index=False)

# %%
# Gerar arquivo com estatístico e consenso solicitado pela Karol, porém unificando os dados em um unico arquivo
colunas_grupo = ['COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'REGIONAL', 'REGIONAL_GESTOR', 'PERIODO', 'CICLO']
colunas_valor = ['QTD_CONSENSO', 'VOL_CONSENSO_DESAGREGADO', 'QTD_ESTATISTICO', 'VOL_ESTATISTICO']
df_plan_estatistico_consenso = df_plano_final_krona.groupby(colunas_grupo, as_index=False)[colunas_valor].sum()

# Renomerar colunas para o formato solicitado
df_plan_estatistico_consenso.rename(columns={
    'QTD_CONSENSO': 'QTD_DEMANDA_CONSENSO',
    'VOL_CONSENSO_DESAGREGADO': 'VOL_DEMANDA_CONSENSO',
    'QTD_ESTATISTICO': 'QTD_PREVISAO_ESTATISTICA',
    'VOL_ESTATISTICO': 'VOL_PREVISAO_ESTATISTICA'
}, inplace=True)

# Apagar o arquivo antigo da pasta de staging caso exista, que tenha no nome plano_saida_estatistico_consenso_
for arquivo in pasta_staging_parquet.glob(f'plano_saida_estatistico_consenso_*.xlsx'):
    arquivo.unlink()

# Salvar arquivo Excel
caminho_saida_excel_estatistico_consenso = pasta_staging_parquet / f'plano_saida_estatistico_consenso_{ciclo_plano}.xlsx'
df_plan_estatistico_consenso.to_excel(caminho_saida_excel_estatistico_consenso, index=False)

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


