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

pasta_hist_vend_prev_est = caminho_base.parent / '03_HIST_VEND_PREV_EST' # Armazena arquivos histórico de vendas processados e separados por Cliente e Produto, e processados para previsão estatística. Armazena Parquet com Previsão Estatística para não consumir memória
pasta_input_parquet = caminho_base.parent / '01_INPUT/01_BD_PARQUET'
pasta_hist_vend_prev_est = caminho_base.parent / '03_HIST_VEND_PREV_EST'
pasta_painel = caminho_base.parent / '04_PAINEL'
pasta_historico_planos = caminho_base.parent / '05_HISTORICO_PLANOS'

print("✅ Mapeamento de pastas concluído com sucesso!")

# %%
# 📥 Importando e preparando arquivos para desagregação

# Ler arquivos em parquet
df_forecast_vendas_krona_PRODUTO = pd.read_parquet(pasta_hist_vend_prev_est / 'df_forecast_vendas_krona_PRODUTO.parquet')
df_forecast_vendas_krona_CLIENTE = pd.read_parquet(pasta_hist_vend_prev_est / 'df_forecast_vendas_krona_CLIENTE.parquet')

# Ler arquivos CSV de planos de demanda
df_plano_consenso_regional = pd.read_csv(
    pasta_historico_planos / 'PLANO_REGIONAL.csv',
    sep=';',              # separador de colunas
    decimal=',',          # vírgula como separador decimal
    thousands='.',        # ponto como separador de milhar
    engine='python'
)

df_plano_consenso_regional_gestor = pd.read_csv(
    pasta_historico_planos / 'PLANO_REGIONAL_GESTOR.csv',
    sep=';',              # separador de colunas
    decimal=',',          # vírgula como separador decimal
    thousands='.',        # ponto como separador de milhar
    engine='python'
)

df_plano_consenso_cliente = pd.read_csv(
    pasta_historico_planos / 'PLANO_CLIENTE.csv',
    sep=';',               # separador de colunas
    decimal=',',           # vírgula como separador decimal
    thousands='.',         # ponto como separador de milhar
    engine='python',
    dtype={'COD_GRP_CLIENTE': str}   # força essa coluna como string
)

# Padronizar PERIODO como datetime
df_plano_consenso_regional['PERIODO'] = pd.to_datetime(df_plano_consenso_regional['PERIODO'])
df_plano_consenso_regional_gestor['PERIODO'] = pd.to_datetime(df_plano_consenso_regional_gestor['PERIODO'])
df_plano_consenso_cliente['PERIODO'] = pd.to_datetime(df_plano_consenso_cliente['PERIODO'])

# Unificar dataframes de df_plano_consenso_regional e df_plano_consenso_regional_gestor
df_plano_consenso_regional_unificado = pd.concat([df_plano_consenso_regional, df_plano_consenso_regional_gestor], ignore_index=True)

# Agrupar dados df_plano_consenso_regional_unificado somando os valores de DEMANDA_PLANEJADA
colunas_agrupamento = ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO']
df_plano_consenso_regional_unificado_grouped = df_plano_consenso_regional_unificado.groupby(colunas_agrupamento)['VALOR'].sum().reset_index()

# Agrupar dados df_plano_consenso_cliente somando os valores de DEMANDA_PLANEJADA
colunas_agrupamento = ['COD_GRP_CLIENTE', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO']
df_plano_consenso_cliente_grouped = df_plano_consenso_cliente.groupby(colunas_agrupamento)['VALOR'].sum().reset_index()

# Guardar variavel versao do plano da coluna VERSAO_PLANO, retornando um valor da coluna VERSAO_PLANO do df_plano_consenso_regional
versao_plano = df_plano_consenso_regional['VERSAO_PLANO'].iloc[0]

print("✅ Arquivos importados e preparados com sucesso!")

# %%
# 📥 Desagregação dos plano REGIONAL

# Calculando a participação da cada linha de produto no volume total da combinação de chaves - REGIONAL_GESTOR, REGIONAL, FAMILIA, PERIODO
df_volume_desag_regional = df_forecast_vendas_krona_PRODUTO.copy()

chaves = ['REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO']

# total por combinação
df_volume_desag_regional['TOTAL'] = df_volume_desag_regional.groupby(chaves)['VOL_VENDA'].transform('sum')

# participação da linha dentro da combinação
df_volume_desag_regional['PARTIC'] = np.where(
    df_volume_desag_regional['TOTAL'] > 0,
    df_volume_desag_regional['VOL_VENDA'] / df_volume_desag_regional['TOTAL'],
    0
)

# Mesclar participação com o plano consenso regional unificado
df_volume_desag_regional = pd.merge(
    df_volume_desag_regional,
    df_plano_consenso_regional_unificado_grouped,
    on=chaves,
    how='left'
)

# Renomear colunas VALOR E VOL_VENDA para evitar conflitos
df_volume_desag_regional.rename(columns={'VOL_VENDA': 'VOL_ESTATISTICO'}, inplace=True)
df_volume_desag_regional.rename(columns={'VALOR': 'VOL_CONSENSO'}, inplace=True)

# Calcular desagregação
df_volume_desag_regional['VOL_CONSENSO'] = df_volume_desag_regional['PARTIC'] * df_volume_desag_regional['VOL_CONSENSO']

# Excluir colunas PESO_UNIT, TOTAL e PARTIC
df_volume_desag_regional.drop(columns=['PESO_UNIT', 'TOTAL', 'PARTIC'], inplace=True)

# %%
# 📥 Desagregação dos plano CLIENTE

# Calculando a participação da cada linha de produto no volume total da combinação de chaves - REGIONAL_GESTOR, REGIONAL, FAMILIA, PERIODO
df_volume_desag_cliente = df_forecast_vendas_krona_CLIENTE.copy()

# Renomear coluna COD_GRP_CLIENTE para COD_GRUPO_CLIENTE para manter padrão
df_volume_desag_cliente.rename(columns={'COD_GRUPO_CLIENTE': 'COD_GRP_CLIENTE'}, inplace=True)

chaves = ['COD_GRP_CLIENTE', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO']

# total por combinação
df_volume_desag_cliente['TOTAL'] = df_volume_desag_cliente.groupby(chaves)['VOL_VENDA'].transform('sum')

# participação da linha dentro da combinação
df_volume_desag_cliente['PARTIC'] = np.where(
    df_volume_desag_cliente['TOTAL'] > 0,
    df_volume_desag_cliente['VOL_VENDA'] / df_volume_desag_cliente['TOTAL'],
    0
)

# Mesclar participação com o plano consenso regional unificado
df_volume_desag_cliente = pd.merge(
    df_volume_desag_cliente,
    df_plano_consenso_cliente_grouped,
    on=chaves,
    how='left'
)

# Renomear colunas VALOR E VOL_VENDA para evitar conflitos
df_volume_desag_cliente.rename(columns={'VOL_VENDA': 'VOL_ESTATISTICO'}, inplace=True)
df_volume_desag_cliente.rename(columns={'VALOR': 'VOL_CONSENSO'}, inplace=True)

# Calcular desagregação
df_volume_desag_cliente['VOL_CONSENSO'] = df_volume_desag_cliente['PARTIC'] * df_volume_desag_cliente['VOL_CONSENSO']

# Excluir colunas TOTAL e PARTIC
df_volume_desag_cliente.drop(columns=['TOTAL', 'PARTIC'], inplace=True)

print("✅ Desagregação concluída com sucesso!")

# %%
# 📥 Unificação de dados CLIENTE E REGIONAL

# Agrupar dados df_volume_desag_cliente
colunas_agrupar = ['EMPRESA', 'COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'REGIONAL', 'REGIONAL_GESTOR', 'PERIODO']

df_volume_desag_cliente_agrupado = df_volume_desag_cliente.groupby(colunas_agrupar).agg({
    'VOL_ESTATISTICO': 'sum',
    'VOL_CONSENSO': 'sum'
}).reset_index()

# Unificar dataframes de df_volume_desag_regional e df_volume_desag_cliente_agrupado
df_plano_final_krona = pd.concat([df_volume_desag_regional, df_volume_desag_cliente_agrupado], ignore_index=True)

# Agrupar dados df_plano_final_krona somando os valores de VOL_ESTATISTICO e VOL_CONSENSO
colunas_agrupamento_final = ['EMPRESA', 'COD_PROD', 'DESC_PRODUTO', 'FAMILIA', 'LINHA', 'REGIONAL', 'REGIONAL_GESTOR', 'PERIODO']
df_plano_final_krona = df_plano_final_krona.groupby(colunas_agrupamento_final).agg({
    'VOL_ESTATISTICO': 'sum',
    'VOL_CONSENSO': 'sum'
}).reset_index()

# Carregar parquet Dim_Produtos_Vendas_krona para buscar peso unitário
df_dim_produtos_vendas_krona = pd.read_parquet(pasta_input_parquet / 'Dim_Produtos_Vendas_krona.parquet')
df_dim_produtos_vendas_krona['Cod_Produto'] = df_dim_produtos_vendas_krona['Cod_Produto'].astype(str)
df_dim_produtos_vendas_krona['Nom_Empresa'] = df_dim_produtos_vendas_krona['Nom_Empresa'].str.upper()

# Manter somente colunas necessárias
colunas_dim_produtos = ['Cod_Produto', 'Nom_Empresa', 'Num_Peso']
df_dim_produtos_vendas_krona = df_dim_produtos_vendas_krona[colunas_dim_produtos]

# Renomear colunas para facilitar merge
df_dim_produtos_vendas_krona.rename(columns={'Cod_Produto': 'COD_PROD', 'Nom_Empresa': 'EMPRESA', 'Num_Peso': 'PESO_UNIT'}, inplace=True)

# Mesclar peso unitário com o plano final
df_plano_final_krona = pd.merge(
    df_plano_final_krona,
    df_dim_produtos_vendas_krona,
    on=['EMPRESA', 'COD_PROD'],
    how='left'
)

df_plano_final_krona['QTD_CONSENSO'] = df_plano_final_krona['VOL_CONSENSO'] / df_plano_final_krona['PESO_UNIT']
df_plano_final_krona['QTD_ESTATISTICO'] = df_plano_final_krona['VOL_ESTATISTICO'] / df_plano_final_krona['PESO_UNIT']

# Adicionar coluna de versão do plano
df_plano_final_krona['VERSAO_PLANO'] = versao_plano

# Carregar parquet com Demanda de Lançamento
df_produtos_lancamento = pd.read_parquet(pasta_hist_vend_prev_est / 'DEMANDA_LANCAMENTO_PRODUTOS_KRONA.parquet')

# Renomear colunas para facilitar merge
df_produtos_lancamento.rename(columns={'CD': 'EMPRESA'}, inplace=True)

# Filtrar colunda EMPRESA de df_produtos_lancamento que contenha KRONA
df_produtos_lancamento = df_produtos_lancamento[df_produtos_lancamento['EMPRESA'].str.contains('KRONA')]

# Filtrar PERIODO da df_produtos_lancamento pelas datas que constam na df_plano_final_krona
periodos_plano = df_plano_final_krona['PERIODO'].unique()
df_produtos_lancamento = df_produtos_lancamento[df_produtos_lancamento['PERIODO'].isin(periodos_plano)].reset_index(drop=True)

# Expandir produtos de lançamento para cobrir todos os períodos do plano
def expandir_produtos_lancamento(df_produtos_lancamento, periodos_plano):
    """
    Expande o dataframe de lançamentos para cobrir todos os períodos do plano.
    Se faltar período, duplica a demanda do último mês disponível.
    Adiciona coluna STATUS para diferenciar demanda existente e criada.
    """
    def expandir_grupo(grupo):
        grupo = grupo.sort_values('PERIODO')
        ultimo_valor = grupo['QTD'].iloc[-1]
        existentes = grupo['PERIODO'].unique()
        faltantes = [p for p in periodos_plano if p not in existentes]

        grupo['STATUS'] = 'DEMANDA_EXISTENTE'

        if faltantes:
            novos = pd.DataFrame({
                'COD_PROD': grupo['COD_PROD'].iloc[0],
                'EMPRESA': grupo['EMPRESA'].iloc[0],
                'PERIODO': faltantes,
                'QTD': ultimo_valor,
                'STATUS': 'DEMANDA_CRIADA'
            })
            grupo = pd.concat([grupo, novos], ignore_index=True)

        return grupo

    # 🚀 iterando manualmente pelos grupos (sem apply → sem warning)
    grupos_expandidos = []
    for _, grupo in df_produtos_lancamento.groupby(['COD_PROD','EMPRESA']):
        grupos_expandidos.append(expandir_grupo(grupo))

    df_expandido = pd.concat(grupos_expandidos, ignore_index=True)
    df_expandido = df_expandido.sort_values(['COD_PROD','EMPRESA','PERIODO'])

    return df_expandido

df_lancamentos_expandido = expandir_produtos_lancamento(df_produtos_lancamento, periodos_plano)

# Salvar df_lancamentos_expandido em excel
df_lancamentos_expandido.to_excel(pasta_historico_planos / f'PLANO_LANCAMENTOS_EXPANDIDO.xlsx', index=False)

# Criar colunas no df_lancamentos_expandido que faltam para unir com o df_plano_final_krona
df_lancamentos_expandido['DESC_PRODUTO'] = ''
df_lancamentos_expandido['FAMILIA'] = ''
df_lancamentos_expandido['LINHA'] = ''
df_lancamentos_expandido['REGIONAL'] = ''
df_lancamentos_expandido['REGIONAL_GESTOR'] = ''
df_lancamentos_expandido['VOL_ESTATISTICO'] = 0
df_lancamentos_expandido['VOL_CONSENSO'] = 0
df_lancamentos_expandido['PESO_UNIT'] = 0
df_lancamentos_expandido['QTD_ESTATISTICO'] = 0
df_lancamentos_expandido['VERSAO_PLANO'] = versao_plano

# Renomear coluna QTD para QTD_CONSENSO
df_lancamentos_expandido.rename(columns={'QTD': 'QTD_CONSENSO'}, inplace=True)

# Ordenar colunas do df_lancamentos_expandido para ficar igual ao df_plano_final_krona
colunas_ordem = df_plano_final_krona.columns.tolist()
df_lancamentos_expandido = df_lancamentos_expandido[colunas_ordem]

# Unir df_plano_final_krona com df_lancamentos_expandido
df_plano_final_krona = pd.concat([df_plano_final_krona, df_lancamentos_expandido], ignore_index=True)

# Salvar em um banco de dados em Parquet, adicionando a versão do plano na nomenclatura do arquivo
df_plano_final_krona.to_parquet(pasta_historico_planos / "BD_PLANOS" / f'PLANO_CONSENSO_KRONA_{versao_plano}.parquet', index=False)

# Agrupar dados e gerar plano para calculo de produção
df_demanda_plano_producao = df_plano_final_krona.groupby(['COD_PROD', 'EMPRESA', 'PERIODO']).agg({
    'QTD_CONSENSO': 'sum'
}).reset_index()

# Salvar plano de demanda em Excel, gerando um arquivo por EMPRESA, excluir coluna EMPRESA e renomear as colunas antes de enviar
for empresa, grupo in df_demanda_plano_producao.groupby('EMPRESA'):
    grupo['Alm.'] = 1
    grupo = grupo.drop(columns=['EMPRESA']).rename(columns={
        'COD_PROD': 'Cod',
        'PERIODO': 'Dt.',
        'QTD_CONSENSO': 'Qtde.'
    })
    grupo = grupo[['Cod', 'Alm.', 'Qtde.', 'Dt.']]
    grupo['Dt.'] = grupo['Dt.'].dt.date

    grupo.to_csv(
        pasta_historico_planos / 'PLANOS_PCP' / f'DEMANDA_LTP_{empresa}_{versao_plano}.csv',
        index=False,
        sep=';',
        decimal=','
    )

    # grupo.to_excel(pasta_historico_planos / 'PLANOS_PCP' / f'DEMANDA_LTP_{empresa}_{versao_plano}.xlsx', index=False)

print("✅ Volumes gerados, bases geradas, plano para produção gerado com sucesso!")

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


