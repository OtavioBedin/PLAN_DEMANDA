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
import gc

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

pasta_staging_parquet = caminho_base.parent / '02_STAGING_PARQUET' # Armazena arquivos histórico de vendas processados e separados por Cliente e Produto, e processados para previsão estatística. Armazena Parquet com Previsão Estatística para não consumir memória
pasta_painel = caminho_base.parent / '05_PAINEL'
pasta_historico_planos = caminho_base.parent / '04_HISTORICO_PLANOS'
pasta_input_painel = caminho_base.parent / '03_INPUT_PAINEL'
origem = pasta_painel / 'PREV_DEMANDA_KRONA.xlsb'
copia = pasta_painel / 'PREV_DEMANDA_KRONA_TEMP.xlsb'

print("✅ Mapeamento de pastas concluído com sucesso!")

# %%
# 📥 Importando o plano Regional

if copia.exists():
    copia.unlink()

shutil.copy2(origem, copia)

df_plano_regional = pd.read_excel(
    copia,
    sheet_name='PLANO_FINAL_COLAB_REGIONAL',
    engine='pyxlsb'
)

df_plano_regional['PERIODO'] = pd.to_datetime(
    df_plano_regional['PERIODO'],
    unit='D',
    origin='1899-12-30'
)

if copia.exists():
    copia.unlink()

parquet_path = pasta_historico_planos / "BD_PLANO_AGREGADO_PAINEL_REGIONAL.parquet"

chaves = ['CICLO', 'REVISAO']

# Padronizar tipos para comparação consistente
df_novo = df_plano_regional.copy()
df_novo['CICLO'] = df_novo['CICLO'].astype(str).str.strip()
df_novo['REVISAO'] = df_novo['REVISAO'].astype(str).str.strip()

pares_novos = df_novo[chaves].drop_duplicates()

if parquet_path.exists():
    df_antigo = pd.read_parquet(parquet_path).copy()

    df_antigo['CICLO'] = df_antigo['CICLO'].astype(str).str.strip()
    df_antigo['REVISAO'] = df_antigo['REVISAO'].astype(str).str.strip()

    # Remove do antigo tudo que tenha (CICLO, REVISAO) que chegou no novo
    df_antigo_filtrado = df_antigo.merge(
        pares_novos,
        on=chaves,
        how='left',
        indicator=True
    ).query("_merge == 'left_only'").drop(columns="_merge")

    df_atualizado = pd.concat([df_antigo_filtrado, df_novo], ignore_index=True)
else:
    df_atualizado = df_novo

df_atualizado.to_parquet(parquet_path, index=False)

print("✅ Plano Regional salvo com sucesso!")

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
    sheet_name='PLANO_FINAL_COLAB_CLIENTE',
    engine='pyxlsb'
)

# ✅ Indicador de existência de plano
if df_plano_cliente['VALOR'].sum() > 0:
    # Converter a coluna PERIODO serial Excel para datetime
    df_plano_cliente['PERIODO'] = pd.to_datetime(
        df_plano_cliente['PERIODO'],
        unit='D',
        origin='1899-12-30'
    )

# Apaga a cópia após leitura
if copia.exists():
    copia.unlink()

parquet_path = pasta_historico_planos / "BD_PLANO_AGREGADO_PAINEL_CLIENTE.parquet"

chaves = ['CICLO', 'REVISAO']

# Padronizar tipos para comparação consistente
df_novo = df_plano_cliente.copy()
df_novo['CICLO'] = df_novo['CICLO'].astype(str).str.strip()
df_novo['REVISAO'] = df_novo['REVISAO'].astype(str).str.strip()

pares_novos = df_novo[chaves].drop_duplicates()

if parquet_path.exists():
    df_antigo = pd.read_parquet(parquet_path).copy()

    df_antigo['CICLO'] = df_antigo['CICLO'].astype(str).str.strip()
    df_antigo['REVISAO'] = df_antigo['REVISAO'].astype(str).str.strip()

    # Remove do antigo tudo que tenha (CICLO, REVISAO) que chegou no novo
    df_antigo_filtrado = df_antigo.merge(
        pares_novos,
        on=chaves,
        how='left',
        indicator=True
    ).query("_merge == 'left_only'").drop(columns="_merge")

    # Concatena antigo (filtrado) + novo
    df_atualizado = pd.concat([df_antigo_filtrado, df_novo], ignore_index=True)

else:
    df_atualizado = df_novo

# Salva o parquet atualizado
df_atualizado.to_parquet(parquet_path, index=False)

print("✅ Plano Cliente salvo com sucesso!")

# %%
# Gerar arquivo CSV de PLANO CICLO ANTERIOR na pasta de INPUT do Painel, para carregar o painel do próximo ciclo, para o plano regional

# Ler parquet plano 
df_plano_consenso_regional = pd.read_parquet(pasta_historico_planos / 'BD_PLANO_AGREGADO_PAINEL_REGIONAL.parquet')

# Retorna ultimo valor da coluna CICLO, para pegar o ciclo mais recente, classificando o PERIODO e a REVISAO
ultimo_ciclo= df_plano_consenso_regional.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['CICLO']
ultima_revisao = df_plano_consenso_regional.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['REVISAO']

# Filtrar os planos agregados para o ciclo mais recente e revisão mais recente
df_plano_consenso_regional = df_plano_consenso_regional[(df_plano_consenso_regional['CICLO'] == ultimo_ciclo) & (df_plano_consenso_regional['REVISAO'] == ultima_revisao)]

# Criar coluna ID na primeira coluna do dataframe, concatenando as colunas REGIONAL, FAMILIA, PERIODO no formato MAR26, separando por traço
df_plano_consenso_regional['ID'] = df_plano_consenso_regional['REGIONAL'] + '-' + df_plano_consenso_regional['FAMILIA'] + '-' + df_plano_consenso_regional['PERIODO'].dt.strftime('%b%y').str.upper()

# Reordenar as colunas para que a coluna ID seja a primeira
cols = ['ID'] + [col for col in df_plano_consenso_regional.columns if col != 'ID']
df_plano_consenso_regional = df_plano_consenso_regional[cols]

# Salvar o plano consenso regional filtrado para o ciclo mais recente e revisão mais recente em CSV na pasta de INPUT do Painel, separado por ponto e vírgula, sem index
df_plano_consenso_regional.to_csv(
    pasta_input_painel / 'PLANO_CICLO_ANTERIOR_REGIONAL.csv',
    sep=';',
    decimal=',',
    index=False,
    encoding='utf-8-sig'
)

del df_plano_consenso_regional
gc.collect()

print("✅ PLANO_CICLO_ANTERIOR_REGIONAL salvo com sucesso!")

# %%
# Gerar arquivo CSV de PLANO CICLO ANTERIOR na pasta de INPUT do Painel, para carregar o painel do próximo ciclo, para o plano Cliente

# Ler parquet plano
df_plano_consenso_cliente = pd.read_parquet(pasta_historico_planos / 'BD_PLANO_AGREGADO_PAINEL_CLIENTE.parquet')

# Se o parquet estiver vazio, salva CSV vazio e não tenta acessar .iloc[0]
if df_plano_consenso_cliente.empty:
    # Criar uma coluna ID vazia para manter a estrutura do arquivo, mesmo sem dados, colocando como primeira coluna do dataframe
    df_plano_consenso_cliente['ID'] = pd.Series(dtype=str)
    
    # Colocar Coluna ID como primeira coluna do dataframe
    cols = ['ID'] + [col for col in df_plano_consenso_cliente.columns if col != 'ID']
    df_plano_consenso_cliente = df_plano_consenso_cliente[cols]
    
    df_plano_consenso_cliente.to_csv(
        pasta_input_painel / 'PLANO_CICLO_ANTERIOR_CLIENTE.csv',
        sep=';',
        decimal=',',
        index=False,
        encoding='utf-8-sig'
    )

else:
    # Retorna ultimo valor da coluna CICLO, para pegar o ciclo mais recente, classificando o PERIODO e a REVISAO
    ultimo_ciclo= df_plano_consenso_cliente.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['CICLO']
    ultima_revisao = df_plano_consenso_cliente.sort_values(by=['PERIODO', 'REVISAO'], ascending=[False, False]).iloc[0]['REVISAO']

    # Filtrar os planos agregados para o ciclo mais recente e revisão mais recente
    df_plano_consenso_cliente = df_plano_consenso_cliente[(df_plano_consenso_cliente['CICLO'] == ultimo_ciclo) & (df_plano_consenso_cliente['REVISAO'] == ultima_revisao)]
    
    # Criar coluna ID na primeira coluna do dataframe, concatenando as colunas REGIONAL, FAMILIA, PERIODO no formato MAR26, separando por traço
    df_plano_consenso_cliente['ID'] = df_plano_consenso_cliente['COD_GRP_CLIENTE'] + '-' + df_plano_consenso_cliente['REGIONAL'] + '-' + df_plano_consenso_cliente['FAMILIA'] + '-' + df_plano_consenso_cliente['PERIODO'].dt.strftime('%b%y').str.upper()

    # Reordenar as colunas para que a coluna ID seja a primeira
    cols = ['ID'] + [col for col in df_plano_consenso_cliente.columns if col != 'ID']
    df_plano_consenso_cliente = df_plano_consenso_cliente[cols]

    # Salvar o plano consenso cliente filtrado para o ciclo mais recente e revisão mais recente em CSV na pasta de INPUT do Painel
    df_plano_consenso_cliente.to_csv(
        pasta_input_painel / 'PLANO_CICLO_ANTERIOR_CLIENTE.csv',
        sep=';',
        decimal=',',
        index=False,
        encoding='utf-8-sig'
    )

    del df_plano_consenso_cliente
    gc.collect()

    print("✅ PLANO_CICLO_ANTERIOR_CLIENTE salvo com sucesso!")

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


