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
# Carregando os planos agregados do painel, para pegar o ciclo mais recente
# e a revisão mais recente para desagregar

# -------------------------------------------------------------------------
# PLANO CONSENSO REGIONAL
# -------------------------------------------------------------------------

arquivo_plano_regional = (
    pasta_historico_planos /
    'BD_PLANO_AGREGADO_PAINEL_REGIONAL.parquet'
)

df_plano_consenso_regional = pd.read_parquet(
    arquivo_plano_regional
)

# Buscar ciclo e revisão mais recentes do plano regional
registro_mais_recente = (
    df_plano_consenso_regional
    .sort_values(
        by=['PERIODO', 'REVISAO'],
        ascending=[False, False]
    )
    .iloc[0]
)

ultimo_ciclo = registro_mais_recente['CICLO']
ultima_revisao = registro_mais_recente['REVISAO']

# Filtrar plano regional
df_plano_consenso_regional = (
    df_plano_consenso_regional[
        (df_plano_consenso_regional['CICLO'] == ultimo_ciclo) &
        (df_plano_consenso_regional['REVISAO'] == ultima_revisao)
    ]
    .copy()
)

df_plano_consenso_regional['PERIODO'] = pd.to_datetime(
    df_plano_consenso_regional['PERIODO']
)

# Agrupar plano regional
colunas_agrupamento_regional = [
    'REGIONAL_GESTOR',
    'REGIONAL',
    'FAMILIA',
    'PERIODO',
    'CICLO'
]

df_plano_consenso_regional_grouped = (
    df_plano_consenso_regional
    .groupby(
        colunas_agrupamento_regional,
        as_index=False
    )['VALOR']
    .sum()
)

ciclo_plano = ultimo_ciclo


# -------------------------------------------------------------------------
# PLANO CONSENSO CLIENTE
# -------------------------------------------------------------------------

arquivo_plano_cliente = (
    pasta_historico_planos /
    'BD_PLANO_AGREGADO_PAINEL_CLIENTE.parquet'
)

existe_plano_cliente = False
df_plano_consenso_cliente_grouped = pd.DataFrame()

if arquivo_plano_cliente.exists():

    df_plano_consenso_cliente = pd.read_parquet(
        arquivo_plano_cliente
    )

    if not df_plano_consenso_cliente.empty:

        df_plano_consenso_cliente['PERIODO'] = pd.to_datetime(
            df_plano_consenso_cliente['PERIODO']
        )

        # Utilizar o mesmo ciclo e revisão selecionados no regional
        df_plano_consenso_cliente = (
            df_plano_consenso_cliente[
                (df_plano_consenso_cliente['CICLO'] == ultimo_ciclo) &
                (
                    df_plano_consenso_cliente['REVISAO']
                    == ultima_revisao
                )
            ]
            .copy()
        )

        # O arquivo pode existir, mas não ter planejamento
        # para o ciclo/revisão atual
        if not df_plano_consenso_cliente.empty:

            colunas_agrupamento_cliente = [
                'COD_CLIENTE',
                'REGIONAL_GESTOR',
                'REGIONAL',
                'FAMILIA',
                'PERIODO',
                'CICLO'
            ]

            df_plano_consenso_cliente_grouped = (
                df_plano_consenso_cliente
                .groupby(
                    colunas_agrupamento_cliente,
                    as_index=False,
                    dropna=False
                )['VALOR']
                .sum()
            )

            existe_plano_cliente = True


# -------------------------------------------------------------------------
# PREVISÃO ESTATÍSTICA POR PRODUTO
# -------------------------------------------------------------------------

df_forecast_vendas_krona_PRODUTO = pd.read_parquet(
    pasta_staging_parquet /
    'df_forecast_vendas_krona_PRODUTO.parquet'
)

df_forecast_vendas_krona_PRODUTO['PERIODO'] = pd.to_datetime(
    df_forecast_vendas_krona_PRODUTO['PERIODO']
)

# Criar identificador único para preservar exatamente a mesma linha
# entre a desagregação regional e a desagregação cliente
df_forecast_vendas_krona_PRODUTO = (
    df_forecast_vendas_krona_PRODUTO
    .reset_index(drop=True)
)

df_forecast_vendas_krona_PRODUTO['ID_LINHA_DESAG'] = (
    df_forecast_vendas_krona_PRODUTO.index
)


# -------------------------------------------------------------------------
# ATUALIZAR DADOS DOS PRODUTOS
# -------------------------------------------------------------------------

df_dim_produtos = pd.read_parquet(
    pasta_staging_parquet /
    'DIM_PRODUTOS_KRONA.parquet'
)

dim_idx = (
    df_dim_produtos
    .drop_duplicates(subset=['COD_PROD'])
    .set_index('COD_PROD')
)

map_cols = {
    'DESC_PROD': 'DESC_PRODUTO',
    'FAMILIA': 'FAMILIA',
    'LINHA': 'LINHA'
}

for col_dim, col_df in map_cols.items():

    novo = (
        df_forecast_vendas_krona_PRODUTO['COD_PROD']
        .map(dim_idx[col_dim])
    )

    if col_df in df_forecast_vendas_krona_PRODUTO.columns:

        df_forecast_vendas_krona_PRODUTO[col_df] = (
            novo.fillna(
                df_forecast_vendas_krona_PRODUTO[col_df]
            )
        )

    else:

        df_forecast_vendas_krona_PRODUTO[col_df] = novo


if existe_plano_cliente:

    print(
        "✅ Arquivos importados. "
        "Plano regional e plano cliente encontrados."
    )

else:

    print(
        "⚠️ Plano cliente não encontrado para o ciclo/revisão atual. "
        "O processo utilizará somente o plano regional."
    )

# %%
# 📥 Desagregação do plano REGIONAL

df_volume_desag_regional = (
    df_forecast_vendas_krona_PRODUTO.copy()
)

chaves_regional = [
    'REGIONAL_GESTOR',
    'REGIONAL',
    'FAMILIA',
    'PERIODO'
]

# Total estatístico dentro da regional, família e período
df_volume_desag_regional['TOTAL_REGIONAL'] = (
    df_volume_desag_regional
    .groupby(
        chaves_regional,
        dropna=False
    )['VOL_PREV']
    .transform('sum')
)

# Participação estatística da linha dentro da regional
df_volume_desag_regional['PARTIC_REGIONAL'] = np.where(
    df_volume_desag_regional['TOTAL_REGIONAL'] > 0,
    (
        df_volume_desag_regional['VOL_PREV'] /
        df_volume_desag_regional['TOTAL_REGIONAL']
    ),
    0
)

# Mesclar com o plano consenso regional
df_volume_desag_regional = pd.merge(
    df_volume_desag_regional,
    df_plano_consenso_regional_grouped,
    on=chaves_regional,
    how='left'
)

df_volume_desag_regional.rename(
    columns={
        'VOL_PREV': 'VOL_ESTATISTICO',
        'VALOR': 'VOL_CONSENSO_REGIONAL'
    },
    inplace=True
)

# Ausência de valor regional será considerada zero
df_volume_desag_regional['VOL_CONSENSO_REGIONAL'] = (
    df_volume_desag_regional['VOL_CONSENSO_REGIONAL']
    .fillna(0)
)

df_volume_desag_regional[
    'VOL_CONSENSO_REGIONAL_DESAGREGADO'
] = (
    df_volume_desag_regional['PARTIC_REGIONAL'] *
    df_volume_desag_regional['VOL_CONSENSO_REGIONAL']
)

print("✅ Desagregação do plano REGIONAL concluída!")

# %%
# 📥 Desagregação do plano CLIENTE

# A desagregação cliente parte exatamente das mesmas linhas
# utilizadas na desagregação regional
df_volume_desag_cliente = (
    df_forecast_vendas_krona_PRODUTO.copy()
)

chaves_cliente = [
    'COD_CLIENTE',
    'REGIONAL_GESTOR',
    'REGIONAL',
    'FAMILIA',
    'PERIODO'
]

# Criar colunas padrão
df_volume_desag_cliente['TOTAL_CLIENTE'] = 0.0
df_volume_desag_cliente['PARTIC_CLIENTE'] = 0.0
df_volume_desag_cliente['VOL_CONSENSO_CLIENTE'] = 0.0

df_volume_desag_cliente[
    'VOL_CONSENSO_CLIENTE_DESAGREGADO'
] = 0.0

df_volume_desag_cliente['PLANO_CLIENTE_ATIVO'] = False


if existe_plano_cliente:

    # Validar se a previsão possui a chave do cliente
    if 'COD_CLIENTE' not in df_volume_desag_cliente.columns:

        raise KeyError(
            "A coluna COD_CLIENTE não existe no arquivo "
            "df_forecast_vendas_krona_PRODUTO.parquet."
        )

    # Total estatístico por cliente, regional, família e período
    df_volume_desag_cliente['TOTAL_CLIENTE'] = (
        df_volume_desag_cliente
        .groupby(
            chaves_cliente,
            dropna=False
        )['VOL_PREV']
        .transform('sum')
    )

    # Participação do produto dentro do planejamento do cliente
    df_volume_desag_cliente['PARTIC_CLIENTE'] = np.where(
        df_volume_desag_cliente['TOTAL_CLIENTE'] > 0,
        (
            df_volume_desag_cliente['VOL_PREV'] /
            df_volume_desag_cliente['TOTAL_CLIENTE']
        ),
        0
    )

    # Mesclar previsão estatística com plano cliente
    df_volume_desag_cliente = pd.merge(
        df_volume_desag_cliente.drop(
            columns=[
                'VOL_CONSENSO_CLIENTE',
                'VOL_CONSENSO_CLIENTE_DESAGREGADO',
                'PLANO_CLIENTE_ATIVO'
            ],
            errors='ignore'
        ),
        df_plano_consenso_cliente_grouped,
        on=chaves_cliente,
        how='left'
    )

    df_volume_desag_cliente.rename(
        columns={
            'VALOR': 'VOL_CONSENSO_CLIENTE'
        },
        inplace=True
    )

    # Cliente sem registro ou com valor nulo será tratado como zero
    df_volume_desag_cliente['VOL_CONSENSO_CLIENTE'] = (
        df_volume_desag_cliente['VOL_CONSENSO_CLIENTE']
        .fillna(0)
    )

    # O plano cliente somente é ativo quando o valor é maior que zero
    #
    # VALOR > 0:
    #     cliente efetivamente realizou o planejamento
    #
    # VALOR = 0:
    #     cliente optou por não realizar o planejamento
    #     e deverá utilizar o regional
    #
    # registro inexistente:
    #     também deverá utilizar o regional
    df_volume_desag_cliente['PLANO_CLIENTE_ATIVO'] = (
        df_volume_desag_cliente['VOL_CONSENSO_CLIENTE'] > 0
    )

    # Desagregar somente os planos cliente efetivamente ativos
    df_volume_desag_cliente[
        'VOL_CONSENSO_CLIENTE_DESAGREGADO'
    ] = np.where(
        df_volume_desag_cliente['PLANO_CLIENTE_ATIVO'],
        (
            df_volume_desag_cliente['PARTIC_CLIENTE'] *
            df_volume_desag_cliente['VOL_CONSENSO_CLIENTE']
        ),
        0
    )

    print("✅ Desagregação do plano CLIENTE concluída!")

else:

    print(
        "⚠️ Não existe plano cliente ativo para o ciclo atual. "
        "A demanda regional será utilizada integralmente."
    )

# %%
# Unificar demanda REGIONAL e CLIENTE

df_plano_final_krona = (
    df_volume_desag_regional.copy()
)

# Trazer somente as informações calculadas na desagregação cliente
colunas_cliente_unificacao = [
    'ID_LINHA_DESAG',
    'TOTAL_CLIENTE',
    'PARTIC_CLIENTE',
    'VOL_CONSENSO_CLIENTE',
    'VOL_CONSENSO_CLIENTE_DESAGREGADO',
    'PLANO_CLIENTE_ATIVO'
]

df_plano_final_krona = pd.merge(
    df_plano_final_krona,
    df_volume_desag_cliente[colunas_cliente_unificacao],
    on='ID_LINHA_DESAG',
    how='left'
)

# Garantir valores padrão quando não existir plano cliente
df_plano_final_krona['TOTAL_CLIENTE'] = (
    df_plano_final_krona['TOTAL_CLIENTE']
    .fillna(0)
)

df_plano_final_krona['PARTIC_CLIENTE'] = (
    df_plano_final_krona['PARTIC_CLIENTE']
    .fillna(0)
)

df_plano_final_krona['VOL_CONSENSO_CLIENTE'] = (
    df_plano_final_krona['VOL_CONSENSO_CLIENTE']
    .fillna(0)
)

df_plano_final_krona[
    'VOL_CONSENSO_CLIENTE_DESAGREGADO'
] = (
    df_plano_final_krona[
        'VOL_CONSENSO_CLIENTE_DESAGREGADO'
    ]
    .fillna(0)
)

df_plano_final_krona['PLANO_CLIENTE_ATIVO'] = (
    df_plano_final_krona['PLANO_CLIENTE_ATIVO']
    .fillna(False)
    .astype(bool)
)

# -------------------------------------------------------------------------
# REGRA FINAL
# -------------------------------------------------------------------------
#
# Se o plano cliente for maior que zero:
#     usar o plano cliente desagregado
#
# Se o plano cliente for zero, nulo ou inexistente:
#     usar o plano regional desagregado
#
df_plano_final_krona['VOL_CONSENSO_DESAGREGADO'] = np.where(
    df_plano_final_krona['PLANO_CLIENTE_ATIVO'],
    df_plano_final_krona[
        'VOL_CONSENSO_CLIENTE_DESAGREGADO'
    ],
    df_plano_final_krona[
        'VOL_CONSENSO_REGIONAL_DESAGREGADO'
    ]
)

df_plano_final_krona['ORIGEM_DEMANDA'] = np.where(
    df_plano_final_krona['PLANO_CLIENTE_ATIVO'],
    'CLIENTE',
    'REGIONAL'
)


# -------------------------------------------------------------------------
# CONVERTER VOLUME PARA PEÇAS
# -------------------------------------------------------------------------

df_dim_produtos = pd.read_parquet(
    pasta_staging_parquet /
    'DIM_PRODUTOS_KRONA.parquet'
)

df_plano_final_krona = pd.merge(
    df_plano_final_krona,
    (
        df_dim_produtos[
            ['COD_PROD', 'PESO_UNIT']
        ]
        .drop_duplicates(subset=['COD_PROD'])
    ),
    on='COD_PROD',
    how='left'
)

# Evitar divisão por zero
df_plano_final_krona['QTD_CONSENSO'] = np.where(
    df_plano_final_krona['PESO_UNIT'] > 0,
    (
        df_plano_final_krona[
            'VOL_CONSENSO_DESAGREGADO'
        ] /
        df_plano_final_krona['PESO_UNIT']
    ),
    0
)

df_plano_final_krona['QTD_ESTATISTICO'] = np.where(
    df_plano_final_krona['PESO_UNIT'] > 0,
    (
        df_plano_final_krona['VOL_ESTATISTICO'] /
        df_plano_final_krona['PESO_UNIT']
    ),
    0
)

df_plano_final_krona['CICLO'] = ciclo_plano

print("✅ Planos REGIONAL e CLIENTE unificados com sucesso!")

# %%


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

print(f"✅ 'plano_saida_gabriel_{ciclo_plano}.xlsx' gerado com sucesso!")

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

print(f"✅ 'plano_saida_gabriel_lancamentos_{ciclo_plano}.xlsx' gerado com sucesso!")

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

print(f"✅ 'plano_saida_estatistico_consenso_{ciclo_plano}.xlsx' gerado com sucesso!")

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


