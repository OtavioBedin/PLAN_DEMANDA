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

pasta_hist_vend_prev_est = caminho_base.parent / '03_HIST_VEND_PREV_EST' # Armazena arquivos histórico de vendas processados e separados por Cliente e Produto, e processados para previsão estatística. Armazena Parquet com Previsão Estatística para não consumir memória
pasta_painel = caminho_base.parent / '04_PAINEL'
pasta_historico_planos = caminho_base.parent / '05_HISTORICO_PLANOS'
origem = pasta_painel / 'PREV_DEMANDA.xlsb'
copia = pasta_painel / 'PREV_DEMANDA_TEMP.xlsb'

print("✅ Mapeamento de pastas concluído com sucesso!")

# %%

# 📥 Importando o plano Regional

# Apaga a cópia anterior, se existir
if copia.exists():
    copia.unlink()

# Copia o arquivo
shutil.copy2(origem, copia)

# Lê a aba desejada
df_plano_regional = pd.read_excel(
    copia,
    sheet_name='PREV_COLAB_REGIONAL',
    engine='pyxlsb',
    # skiprows=3  # pula as 4 primeiras linhas, começa na linha 5
)

# Elimninar Coluna 1
df_plano_regional = df_plano_regional.drop(df_plano_regional.columns[0], axis=1)

# Na index 4, concaternar dados index 5 e index 3, separados por "_"
df_plano_regional.iloc[4] = df_plano_regional.iloc[5].astype(str) + "_" + df_plano_regional.iloc[3].astype(str)

# Avaliar na index 5, e manter apenas os valores que constam na colunas_manter
colunas_manter = ['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA', 'Consenso [KG]']

# Avaliar a linha de índice 5 (sexta linha do DataFrame)
linha = df_plano_regional.iloc[5]

# Loop ligando cada coluna ao valor da linha
for coluna, valor in zip(df_plano_regional.columns, linha):
    if valor not in colunas_manter and not isinstance(valor, pd.Timestamp):
        df_plano_regional = df_plano_regional.drop(columns=[coluna])
        
# Eliminar index 5 e 0 a 3
df_plano_regional = df_plano_regional.drop(index=[0, 1, 2, 3, 5]).reset_index(drop=True)

# Avaliar dados da index 0, eliminar _nan
df_plano_regional.iloc[0] = df_plano_regional.iloc[0].str.replace('_nan', '', regex=False)

# Promover a primeira linha a cabeçalho
df_plano_regional.columns = df_plano_regional.iloc[0]
df_plano_regional = df_plano_regional.drop(index=0).reset_index(drop=True)

# Definir colunas fixas
colunas_fixas = ['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA']

# Melt para transformar colunas em linhas
df_plano_regional = df_plano_regional.melt(
    id_vars=colunas_fixas,
    var_name='Coluna',
    value_name='VALOR'
)

# Preencher valores NaN com 0
df_plano_regional['VALOR'] = df_plano_regional['VALOR'].fillna(0)

# Separar TIPO e PERIODO
df_plano_regional[['TIPO', 'DataSerial']] = df_plano_regional['Coluna'].str.split('_', n=1, expand=True)

# Converter datas (serial Excel → datetime)
df_plano_regional['PERIODO'] = pd.to_datetime(df_plano_regional['DataSerial'].astype(float), unit='d', origin='1899-12-30')

# Organizar colunas finais
df_plano_regional = df_plano_regional[['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO', 'TIPO', 'VALOR']]

# Renomear colunas
df_plano_regional = df_plano_regional.rename(columns={
    'REGIONAL GESTOR': 'REGIONAL_GESTOR',
    'REGIONAL': 'REGIONAL',
    'FAMILIA': 'FAMILIA',
    'PERIODO': 'PERIODO',
    'TIPO': 'TIPO',
    'VALOR': 'VALOR'
})

# Criar ID concatenando REGIONAL + FAMILIA + PERIODO
df_plano_regional['ID'] = df_plano_regional['REGIONAL'] + "-" + df_plano_regional['FAMILIA'] + "-" + df_plano_regional['PERIODO'].dt.strftime('%b%y').str.upper()

# Ordenar colunas
df_plano_regional = df_plano_regional[['ID', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO', 'TIPO', 'VALOR']]

# Criar coluna ORIGEM_PLANO
df_plano_regional['ORIGEM_PLANO'] = 'PLANO_REGIONAL'

print("✅ Plano Regional salvo com sucesso!")

# %%
# 📥 Importando o plano Regional Gestor

# Apaga a cópia anterior, se existir
if copia.exists():
    copia.unlink()

# Copia o arquivo
shutil.copy2(origem, copia)

# Lê a aba desejada
df_plano_regional_gestor = pd.read_excel(
    copia,
    sheet_name='PREV_COLAB_REGIONAL',
    engine='pyxlsb',
    # skiprows=3  # pula as 4 primeiras linhas, começa na linha 5
)

# Elimninar Coluna 1
df_plano_regional_gestor = df_plano_regional_gestor.drop(df_plano_regional_gestor.columns[0], axis=1)

# Na index 4, concaternar dados index 5 e index 3, separados por "_"
df_plano_regional_gestor.iloc[4] = df_plano_regional_gestor.iloc[5].astype(str) + "_" + df_plano_regional_gestor.iloc[3].astype(str)

# Avaliar na index 5, e manter apenas os valores que constam na colunas_manter
colunas_manter = ['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA', 'Consenso Regional Gestor [KG]']

# Avaliar a linha de índice 5 (sexta linha do DataFrame)
linha = df_plano_regional_gestor.iloc[5]

# Loop ligando cada coluna ao valor da linha
for coluna, valor in zip(df_plano_regional_gestor.columns, linha):
    if valor not in colunas_manter and not isinstance(valor, pd.Timestamp):
        df_plano_regional_gestor = df_plano_regional_gestor.drop(columns=[coluna])
        
# Eliminar index 5 e 0 a 3
df_plano_regional_gestor = df_plano_regional_gestor.drop(index=[0, 1, 2, 3, 5]).reset_index(drop=True)

# Avaliar dados da index 0, eliminar _nan
df_plano_regional_gestor.iloc[0] = df_plano_regional_gestor.iloc[0].str.replace('_nan', '', regex=False)

# Promover a primeira linha a cabeçalho
df_plano_regional_gestor.columns = df_plano_regional_gestor.iloc[0]
df_plano_regional_gestor = df_plano_regional_gestor.drop(index=0).reset_index(drop=True)

# Definir colunas fixas
colunas_fixas = ['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA']

# 1. Melt para transformar colunas em linhas
df_plano_regional_gestor = df_plano_regional_gestor.melt(
    id_vars=colunas_fixas,
    var_name='Coluna',
    value_name='VALOR'
)

# Preencher valores NaN com 0
df_plano_regional_gestor['VALOR'] = df_plano_regional_gestor['VALOR'].fillna(0)

# Separar TIPO e PERIODO
df_plano_regional_gestor[['TIPO', 'DataSerial']] = df_plano_regional_gestor['Coluna'].str.split('_', n=1, expand=True)

# Converter datas (serial Excel → datetime)
df_plano_regional_gestor['PERIODO'] = pd.to_datetime(df_plano_regional_gestor['DataSerial'].astype(float), unit='d', origin='1899-12-30')

# Organizar colunas finais
df_plano_regional_gestor = df_plano_regional_gestor[['REGIONAL GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO', 'TIPO', 'VALOR']]

# Renomear colunas
df_plano_regional_gestor = df_plano_regional_gestor.rename(columns={
    'REGIONAL GESTOR': 'REGIONAL_GESTOR',
    'REGIONAL': 'REGIONAL',
    'FAMILIA': 'FAMILIA',
    'PERIODO': 'PERIODO',
    'TIPO': 'TIPO',
    'VALOR': 'VALOR'
})

# Criar ID concatenando REGIONAL_GESTOR + FAMILIA + PERIODO
df_plano_regional_gestor['ID'] = df_plano_regional_gestor['REGIONAL_GESTOR'] + "-" + df_plano_regional_gestor['FAMILIA'] + "-" + df_plano_regional_gestor['PERIODO'].dt.strftime('%b%y').str.upper()

# Ordenar colunas
df_plano_regional_gestor = df_plano_regional_gestor[['ID', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'PERIODO', 'TIPO', 'VALOR']]

# Criar coluna ORIGEM_PLANO
df_plano_regional_gestor['ORIGEM_PLANO'] = 'PLANO_REGIONAL_GESTOR'

print("✅ Plano Regional Gestor salvo com sucesso!")

# %%

# 📥 Importando o plano Cliente

# Apaga a cópia anterior, se existir
if copia.exists():
    copia.unlink()

# Copia o arquivo
shutil.copy2(origem, copia)

# Lê a aba desejada
df_plano_cliente = pd.read_excel(
    copia,
    sheet_name='PREV_COLAB_CLIENTE',
    engine='pyxlsb',
    # skiprows=3  # pula as 4 primeiras linhas, começa na linha 5
)

# Elimninar Coluna 1
df_plano_cliente = df_plano_cliente.drop(df_plano_cliente.columns[0], axis=1)

# Na index 4, concaternar dados index 5 e index 3, separados por "_"
df_plano_cliente.iloc[4] = df_plano_cliente.iloc[5].astype(str) + "_" + df_plano_cliente.iloc[3].astype(str)

# Avaliar na index 5, e manter apenas os valores que constam na colunas_manter
colunas_manter = ['REGIONAL GESTOR', 'REGIONAL', 'COD GRP CLIENTE', 'DESC GRP CLIENTE', 'FAMILIA', 'Consenso [KG]']

# Avaliar a linha de índice 5 (sexta linha do DataFrame)
linha = df_plano_cliente.iloc[5]

# Loop ligando cada coluna ao valor da linha
for coluna, valor in zip(df_plano_cliente.columns, linha):
    if valor not in colunas_manter and not isinstance(valor, pd.Timestamp):
        df_plano_cliente = df_plano_cliente.drop(columns=[coluna])
        
# Eliminar index 5 e 0 a 3
df_plano_cliente = df_plano_cliente.drop(index=[0, 1, 2, 3, 5]).reset_index(drop=True)

# Avaliar dados da index 0, eliminar _nan
df_plano_cliente.iloc[0] = df_plano_cliente.iloc[0].str.replace('_nan', '', regex=False)

# Promover a primeira linha a cabeçalho
df_plano_cliente.columns = df_plano_cliente.iloc[0]
df_plano_cliente = df_plano_cliente.drop(index=0).reset_index(drop=True)

# Definir colunas fixas
colunas_fixas = ['REGIONAL GESTOR', 'REGIONAL', 'COD GRP CLIENTE', 'DESC GRP CLIENTE', 'FAMILIA',]

# # 1. Melt para transformar colunas em linhas
df_plano_cliente = df_plano_cliente.melt(
    id_vars=colunas_fixas,
    var_name='Coluna',
    value_name='VALOR'
)

# Preencher valores NaN com 0
df_plano_cliente['VALOR'] = df_plano_cliente['VALOR'].fillna(0)

# Separar TIPO e PERIODO
df_plano_cliente[['TIPO', 'DataSerial']] = df_plano_cliente['Coluna'].str.split('_', n=1, expand=True)

# Converter datas (serial Excel → datetime)
df_plano_cliente['PERIODO'] = pd.to_datetime(df_plano_cliente['DataSerial'].astype(float), unit='d', origin='1899-12-30')

# Organizar colunas finais
df_plano_cliente = df_plano_cliente[['REGIONAL GESTOR', 'REGIONAL', 'COD GRP CLIENTE', 'DESC GRP CLIENTE', 'FAMILIA', 'PERIODO', 'TIPO', 'VALOR']]

# Renomear colunas
df_plano_cliente = df_plano_cliente.rename(columns={
    'REGIONAL GESTOR': 'REGIONAL_GESTOR',
    'REGIONAL': 'REGIONAL',
    'FAMILIA': 'FAMILIA',
    'COD GRP CLIENTE': 'COD_GRP_CLIENTE',
    'DESC GRP CLIENTE': 'DESC_GRP_CLIENTE',
    'PERIODO': 'PERIODO',
    'TIPO': 'TIPO',
    'VALOR': 'VALOR'
})

# Padronizar COD_GRP_CLIENTE como string
df_plano_cliente['COD_GRP_CLIENTE'] = df_plano_cliente['COD_GRP_CLIENTE'].astype(str)

# Criar ID concatenando COD_GRP_CLIENTE + REGIONAL + FAMILIA + PERIODO
df_plano_cliente['ID'] = df_plano_cliente['COD_GRP_CLIENTE'] + "-" + df_plano_cliente['REGIONAL'] + "-" + df_plano_cliente['FAMILIA']  + "-" + df_plano_cliente['PERIODO'].dt.strftime('%b%y').str.upper()

# Ordenar colunas
df_plano_cliente = df_plano_cliente[['ID', 'REGIONAL_GESTOR', 'REGIONAL', 'FAMILIA', 'COD_GRP_CLIENTE', 'DESC_GRP_CLIENTE', 'PERIODO', 'TIPO', 'VALOR']]

# Criar coluna ORIGEM_PLANO
df_plano_cliente['ORIGEM_PLANO'] = 'PLANO_CLIENTE'

print("✅ Plano Cliente salvo com sucesso!")

# %%
# 📥 Salvando os arquivos e criando versões dos planos

versao_plano = datetime.now().strftime('%Y%m%d_%H%M%S')

# Criar coluna de versão em cada DataFrame
df_plano_regional['VERSAO_PLANO'] = versao_plano
df_plano_regional_gestor['VERSAO_PLANO'] = versao_plano
df_plano_cliente['VERSAO_PLANO'] = versao_plano

# Formatar coluna VALOR para padrão brasileiro com 3 casas decimais - PLANO REGIONAL
df_plano_regional['VALOR'] = df_plano_regional['VALOR'].apply(
    lambda x: f"{x:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
)

df_plano_regional.to_csv(
    pasta_historico_planos / 'PLANO_REGIONAL.csv',
    index=False,
    sep=';'
)

# Formatar coluna VALOR para padrão brasileiro com 3 casas decimais - PLANO REGIONAL GESTOR
df_plano_regional_gestor['VALOR'] = df_plano_regional_gestor['VALOR'].apply(
    lambda x: f"{x:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".") 
)

df_plano_regional_gestor.to_csv(
    pasta_historico_planos / 'PLANO_REGIONAL_GESTOR.csv',
    index=False,
    sep=';'
)

# Formatar coluna VALOR para padrão brasileiro com 3 casas decimais - PLANO CLIENTE
df_plano_cliente['VALOR'] = df_plano_cliente['VALOR'].apply(
    lambda x: f"{x:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".") 
)

df_plano_cliente.to_csv(
    pasta_historico_planos / 'PLANO_CLIENTE.csv',
    index=False,
    sep=';'
)

# Apaga o arquivo PREV_DEMANDA_TEMP, se existir
if copia.exists():
    copia.unlink()

print("✅ Planos de Demanda Consolidados e salvos com sucesso!")

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


