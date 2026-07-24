# %%
# ============================================================
# SETUP - Forecast Estatístico BI
# ============================================================
import os

# Limita threads internas para evitar briga com joblib/Power BI
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

from functions import *

import pandas as pd
import numpy as np
import locale
import warnings
import logging
import time

from pathlib import Path
from datetime import datetime
from joblib import Parallel, delayed

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_percentage_error
from statsmodels.tsa.holtwinters import ExponentialSmoothing

logging.basicConfig(level=logging.WARNING, format='%(message)s')
warnings.filterwarnings("ignore")

timer = Temporizador()
timer.iniciar()

try:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')  # Windows
except Exception:
    try:
        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')  # Linux/Mac
    except Exception:
        pass

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)
pd.set_option('display.expand_frame_repr', False)

# Detecta se o script está sendo executado de um .py ou de um notebook
try:
    caminho_base = Path(__file__).resolve().parent
except NameError:
    caminho_base = Path.cwd()

pasta_input_parquet = caminho_base.parent / '01_INPUT_PIPELINE/01_BD_PARQUET'
arquivo_input_regras_negocio = caminho_base.parent / '01_INPUT_PIPELINE/02_REGRAS_NEGOCIO/KRONA_REGRAS.xlsm'
pasta_staging_parquet = caminho_base.parent / '02_STAGING_PARQUET'
pasta_input_painel = caminho_base.parent / '03_INPUT_PAINEL'
pasta_painel = caminho_base.parent / '05_PAINEL'
pasta_validacao_anna = caminho_base.parent / '06_VALIDACAO_ANNA'

print("✅ Setup e mapeamento de pastas concluídos com sucesso!")

# %%
# Carregar dados arquivo KRONA_REGRAS
caminho_arquivo = arquivo_input_regras_negocio

#-----------------------------------------------------------------------#
#---------------Carregar Regionais Gestor ------------------------------#
#-----------------------------------------------------------------------#
guia_excel = 'REGIONAIS_GESTOR'
df_regionais_gestor = pd.read_excel(caminho_arquivo, sheet_name=guia_excel, engine='calamine')
df_regionais_gestor = df_regionais_gestor.drop_duplicates(subset=['REGIONAL', 'REGIONAL_GESTOR'])
df_regionais_gestor = df_regionais_gestor[df_regionais_gestor['REGIONAL'].notna()].reset_index(drop=True)

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
# ============================================================
# VALIDAÇÃO DAS FONTES MÍNIMAS DO FORECAST
# ============================================================

arquivo_vendas = pasta_staging_parquet / "df_vendas_krona.parquet"
arquivo_dim_produtos = pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet"
arquivo_dim_produtos_origem = pasta_staging_parquet / "Dim_Produtos_Vendas_Krona.parquet"

if not arquivo_vendas.exists():
    raise FileNotFoundError(
        f"Fonte obrigatória não encontrada: {arquivo_vendas}\n"
        "Este notebook isolado espera que o tratamento de vendas já tenha gerado df_vendas_krona.parquet."
    )

if not arquivo_dim_produtos.exists():
    if arquivo_dim_produtos_origem.exists():
        print("⚠️ DIM_PRODUTOS_KRONA.parquet não encontrada. Criando a partir de Dim_Produtos_Vendas_Krona.parquet...")

        dim_produtos_origem = pd.read_parquet(arquivo_dim_produtos_origem)

        colunas_necessarias = ["COD_PROD", "DESC_PROD", "PESO_UNIT", "FAMILIA", "LINHA"]
        colunas_faltantes = [c for c in colunas_necessarias if c not in dim_produtos_origem.columns]

        if colunas_faltantes:
            raise ValueError(
                "Não foi possível criar DIM_PRODUTOS_KRONA.parquet. "
                f"Colunas faltantes em {arquivo_dim_produtos_origem.name}: {colunas_faltantes}"
            )

        dim_produtos = (
            dim_produtos_origem[colunas_necessarias]
            .drop_duplicates(subset=["COD_PROD"])
            .reset_index(drop=True)
        )

        dim_produtos.to_parquet(arquivo_dim_produtos, index=False)
        print(f"✅ DIM_PRODUTOS_KRONA.parquet criada | Linhas: {len(dim_produtos):,}")
    else:
        raise FileNotFoundError(
            f"Fonte obrigatória não encontrada: {arquivo_dim_produtos}\n"
            f"Também não encontrei a origem alternativa: {arquivo_dim_produtos_origem}\n"
            "Gere/copiei a dimensão de produtos antes de rodar o forecast."
        )
else:
    print(f"✅ DIM_PRODUTOS_KRONA.parquet encontrada: {arquivo_dim_produtos}")

# %%
# FIXME: Código criado para adaptação ao Power BI (Python Script) — ajustes necessários para ambiente e estrutura de dados do cliente, solicitado pelo ALEX
# Código adaptado para arquitetura Power BI
# - Não gera DIM_CALENDARIO (será criada no Power BI via DAX)
# - Gera previsões de TODOS os modelos para o futuro
# - Gera tabela de métricas por série/modelo (opcional)
# - Enriquecimento da DIM_SERIE e fatos completos com QTD
# - Salva tudo em parquet na pasta FORECAST_BI

# ============================================================
# PARÂMETROS
# ============================================================
N_NUCLEOS = 8
PRINT_EVERY = 50
METRICA_USADA = "WAPE"   # "WAPE" ou "MAPE"
MIN_TREINO = 12
STEP_BACKTEST = 6
CALCULAR_METRICAS = True

# Ajuste nomes/pastas conforme seu ambiente
# pasta_staging_parquet = Path(...)
# df_periodo_previsao = ...
# df_regionais_gestor = ...
# timer = ...


# ============================================================
# MÉTRICAS
# ============================================================
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
# MODELOS
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
    rf = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=1,
        max_depth=12,
        min_samples_leaf=2,
    )
    rf.fit(X_train, y_train)

    X_pred = _make_X(period_index_pred, len(period_index_train))
    return np.maximum(rf.predict(X_pred), 0)


def pred_gb(period_index_train, y_train, period_index_pred):
    X_train = _make_X(period_index_train, 0)
    gb = GradientBoostingRegressor(random_state=42)
    gb.fit(X_train, y_train)

    X_pred = _make_X(period_index_pred, len(period_index_train))
    return np.maximum(gb.predict(X_pred), 0)


def serie_intermitente(y, min_meses=12, min_vendas=4, min_densidade=0.35):
    y = np.asarray(y, dtype=float)
    if len(y) == 0:
        return True

    n_pos = np.count_nonzero(y > 0)
    densidade = n_pos / len(y)

    return len(y) < min_meses or n_pos < min_vendas or densidade < min_densidade


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
    else:
        # Série curta/intermitente: média dos últimos 12 meses incluindo zeros
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

    if cap <= 0:
        cap = 0.0

    return np.minimum(np.maximum(fc, 0), cap)


def completar_calendario_mensal(df_hist_base, ultimo_mes_hist, coluna_valor):
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
        g2[coluna_valor] = g2[coluna_valor].fillna(0)

        partes.append(g2[["COD_PROD", "REGIONAL", "PERIODO", coluna_valor]])

    return pd.concat(partes, ignore_index=True)


def run_model(model_name, idx_train, y_train, idx_pred, steps):
    if model_name == "HoltWinters":
        return pred_hw(y_train, steps)
    if model_name == "LinearRegression":
        return pred_lr(y_train, steps)
    if model_name == "RandomForest":
        return pred_rf(idx_train, y_train, idx_pred)
    if model_name == "GradientBoosting":
        return pred_gb(idx_train, y_train, idx_pred)
    if model_name == "Media12_Intermitente":
        return pred_intermitente(y_train, steps)
    raise ValueError(f"Modelo inválido: {model_name}")


MODEL_ORDER = [
    "HoltWinters",
    "LinearRegression",
    "RandomForest",
    "GradientBoosting",
]

MODEL_ID_MAP = {m: i + 1 for i, m in enumerate(MODEL_ORDER)}


# ============================================================
# WORKERS
# ============================================================
def _worker_forecast_all_models(cod_prod, regional, periodos_np, y_np, future_dates_np, horizon):
    idx_hist = pd.DatetimeIndex(periodos_np)
    y = y_np.astype(float)
    idx_future = pd.DatetimeIndex(future_dates_np)

    rows = []
    for model_name in MODEL_ORDER:
        try:
            # Proteção: séries curtas/intermitentes não devem repetir venda pontual
            # como recorrência mensal. Para esses casos, todos os modelos recebem
            # previsão conservadora por média 12M com zeros, identificada no MODELO_EXECUTADO.
            if serie_intermitente(y):
                fc = pred_intermitente(y, horizon)
                model_exec = f"{model_name}_FallbackMedia12_Intermitente"
            else:
                fc = run_model(model_name, idx_hist, y, idx_future, horizon)
                fc = limitar_forecast(fc, y)
                model_exec = model_name
        except Exception:
            fc = pred_intermitente(y, horizon)
            fc = limitar_forecast(fc, y)
            model_exec = f"{model_name}_FallbackMedia12_Intermitente"

        fc = limitar_forecast(fc, y)

        rows.extend([
            [cod_prod, regional, pd.Timestamp(per), model_name, model_exec, float(val)]
            for per, val in zip(future_dates_np, fc)
        ])

    return rows, (cod_prod, regional)


def _worker_metricas_all_models(cod_prod, regional, periodos_np, y_np, min_treino, step_backtest):
    idx = pd.DatetimeIndex(periodos_np)
    y = y_np.astype(float)

    rows_metricas = []

    for model_name in MODEL_ORDER:
        preds = np.full(len(y), np.nan, dtype=float)
        ape = np.full(len(y), np.nan, dtype=float)
        model_exec = model_name

        t = min_treino
        while t < len(y):
            y_train = y[:t]
            idx_train = idx[:t]

            steps = min(step_backtest, len(y) - t)
            idx_pred = idx[t:t + steps]

            try:
                # Backtest com a mesma proteção usada no forecast futuro.
                # Se a janela de treino ainda é curta/intermitente, usa média 12M com zeros.
                if serie_intermitente(y_train):
                    y_preds = pred_intermitente(y_train, steps)
                    model_exec = f"{model_name}_FallbackMedia12_Intermitente"
                else:
                    y_preds = run_model(model_name, idx_train, y_train, idx_pred, steps)
                    y_preds = limitar_forecast(y_preds, y_train)
                    model_exec = model_name
            except Exception:
                y_preds = pred_intermitente(y_train, steps)
                y_preds = limitar_forecast(y_preds, y_train)
                model_exec = f"{model_name}_FallbackMedia12_Intermitente"

            for i_step in range(steps):
                pos = t + i_step
                preds[pos] = max(float(y_preds[i_step]), 0)
                if y[pos] != 0:
                    ape[pos] = abs((y[pos] - preds[pos]) / y[pos])

            t += steps

        mask_pred = ~np.isnan(preds)
        y_valid = y[mask_pred]
        pred_valid = preds[mask_pred]
        metrica_valor = metric(y_valid, pred_valid) if mask_pred.sum() else np.nan
        mape_serie = metrica_valor

        rows_metricas.append([
            cod_prod,
            regional,
            model_name,
            model_exec,
            float(metrica_valor) if pd.notna(metrica_valor) else np.nan,
            float(mape_serie) if pd.notna(mape_serie) else np.nan,
            int(mask_pred.sum()),
        ])

    return rows_metricas, (cod_prod, regional)


# ============================================================
# UTILITÁRIOS DE DIMENSÃO/CHAVE
# ============================================================
def construir_dim_serie(df_group):
    dim_serie = (
        df_group[["COD_PROD", "REGIONAL"]]
        .drop_duplicates()
        .sort_values(["COD_PROD", "REGIONAL"])
        .reset_index(drop=True)
    )
    dim_serie["ID_SERIE"] = np.arange(1, len(dim_serie) + 1, dtype=np.int64)
    return dim_serie[["ID_SERIE", "COD_PROD", "REGIONAL"]]


def construir_dim_modelo():
    return pd.DataFrame(
        {
            "ID_MODELO": [MODEL_ID_MAP[m] for m in MODEL_ORDER],
            "MODELO": MODEL_ORDER,
            "ORDEM_MODELO": np.arange(1, len(MODEL_ORDER) + 1, dtype=np.int16),
        }
    )


def safe_divide(series_num, series_den):
    num = pd.to_numeric(series_num, errors="coerce")
    den = pd.to_numeric(series_den, errors="coerce")
    return np.where((den.notna()) & (den != 0), num / den, np.nan)


# ============================================================
# PÓS-PROCESSAMENTO BI
# ============================================================
def enriquecer_dim_serie(out_dir):
    print("🧩 Enriquecendo DIM_SERIE...")

    dim_serie = pd.read_parquet(out_dir / "DIM_SERIE.parquet")
    dim_serie = dim_serie.merge(df_regionais_gestor, on="REGIONAL", how="left")

    dim_produtos = pd.read_parquet(pasta_staging_parquet / "DIM_PRODUTOS_KRONA.parquet")
    dim_serie = dim_serie.merge(dim_produtos, on="COD_PROD", how="left")

    colunas_ordenadas = [
        "ID_SERIE",
        "COD_PROD",
        "DESC_PROD",
        "PESO_UNIT",
        "FAMILIA",
        "LINHA",
        "REGIONAL",
        "REGIONAL_GESTOR",
    ]
    dim_serie = dim_serie[colunas_ordenadas]

    dim_serie.to_parquet(out_dir / "DIM_SERIE_COMPLETA.parquet", index=False)
    print(f"✅ DIM_SERIE_COMPLETA salva | Linhas: {len(dim_serie):,}")


def enriquecer_fatos_com_qtd(out_dir):
    print("📦 Enriquecendo fatos com PESO_UNIT e quantidade...")

    fato_historico = pd.read_parquet(out_dir / "FATO_HISTORICO.parquet")
    fato_previsao_modelo = pd.read_parquet(out_dir / "FATO_PREVISAO_MODELO.parquet")

    dim_serie_completa = pd.read_parquet(
        out_dir / "DIM_SERIE_COMPLETA.parquet",
        columns=["ID_SERIE", "PESO_UNIT"],
    )

    fato_historico = fato_historico.merge(dim_serie_completa, on="ID_SERIE", how="left")
    fato_historico["QTD_REAL"] = safe_divide(fato_historico["VOL_REAL"], fato_historico["PESO_UNIT"])
    fato_historico.to_parquet(out_dir / "FATO_HISTORICO_COMPLETA.parquet", index=False)

    fato_previsao_modelo = fato_previsao_modelo.merge(dim_serie_completa, on="ID_SERIE", how="left")
    fato_previsao_modelo["QTD_PREV"] = safe_divide(fato_previsao_modelo["VOL_PREV"], fato_previsao_modelo["PESO_UNIT"])
    fato_previsao_modelo.to_parquet(out_dir / "FATO_PREVISAO_MODELO_COMPLETA.parquet", index=False)

    print(f"✅ FATO_HISTORICO_COMPLETA salva | Linhas: {len(fato_historico):,}")
    print(f"✅ FATO_PREVISAO_MODELO_COMPLETA salva | Linhas: {len(fato_previsao_modelo):,}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("🔄 Iniciando processamento para arquitetura BI...")

    # ============================================================
    # 0) CARGA
    # ============================================================
    df_vendas_krona = pd.read_parquet(pasta_staging_parquet / "df_vendas_krona.parquet")
    print(f"📦 df_vendas_krona carregado | Linhas: {len(df_vendas_krona):,}")

    # ============================================================
    # 1) AGRUPAMENTO BASE
    # ============================================================
    df_group = (
        df_vendas_krona
        .groupby(["COD_PROD", "REGIONAL", "PERIODO"], as_index=False, sort=False)
        .agg(VOL_REAL=("VOL_VENDA", "sum"))
        .sort_values(["COD_PROD", "REGIONAL", "PERIODO"])
        .reset_index(drop=True)
    )

    qtd_series = df_group[["COD_PROD", "REGIONAL"]].drop_duplicates().shape[0]
    print(f"📊 Séries identificadas: {qtd_series:,}")

    # ============================================================
    # 2) CALENDÁRIO FUTURO (somente para recorte e previsão)
    # ============================================================
    future_dates = pd.DatetimeIndex(
        df_periodo_previsao["PERIODO_PROJECAO"].drop_duplicates().sort_values()
    )
    if len(future_dates) == 0:
        raise ValueError("df_periodo_previsao['PERIODO_PROJECAO'] está vazio.")

    primeiro_mes_previsao = future_dates.min()
    ultimo_mes_hist = primeiro_mes_previsao - pd.offsets.MonthBegin(1)

    df_hist_base = df_group[df_group["PERIODO"] <= ultimo_mes_hist].copy()
    if df_hist_base.empty:
        raise ValueError("Histórico vazio após corte pelo calendário futuro.")

    # Correção estatística: completa meses sem venda com zero.
    # Sem isso, uma venda isolada pode virar série curta artificial e ser repetida no futuro.
    df_hist_base = completar_calendario_mensal(df_hist_base, ultimo_mes_hist, coluna_valor="VOL_REAL")

    horizon = len(future_dates)
    print(
        f"🗓️ Horizonte futuro: {horizon} meses | "
        f"{future_dates.min().date()} → {future_dates.max().date()}"
    )

    # ============================================================
    # 3) DIMENSÕES
    # ============================================================
    dim_serie = construir_dim_serie(df_hist_base)
    dim_modelo = construir_dim_modelo()

    serie_map = dim_serie.set_index(["COD_PROD", "REGIONAL"])["ID_SERIE"].to_dict()

    # ============================================================
    # 4) FATO HISTÓRICO
    # ============================================================
    fato_historico = df_hist_base.copy()
    fato_historico["ID_SERIE"] = fato_historico.set_index(["COD_PROD", "REGIONAL"]).index.map(serie_map).astype(np.int64)
    fato_historico = fato_historico[["ID_SERIE", "PERIODO", "VOL_REAL"]].sort_values(["ID_SERIE", "PERIODO"]).reset_index(drop=True)
    print(f"📚 FATO_HISTORICO pronta | Linhas: {len(fato_historico):,}")

    # ============================================================
    # 5) PREVISÃO FUTURA - TODOS OS MODELOS
    # ============================================================
    tasks = []
    for (cod_prod, regional), df_serie in df_hist_base.groupby(["COD_PROD", "REGIONAL"], sort=False):
        df_serie = df_serie.sort_values("PERIODO")
        tasks.append((
            cod_prod,
            regional,
            df_serie["PERIODO"].to_numpy(dtype="datetime64[ns]"),
            df_serie["VOL_REAL"].to_numpy(dtype=float),
        ))

    total_series = len(tasks)
    future_dates_np = future_dates.to_numpy(dtype="datetime64[ns]")

    print(f"🚀 Gerando previsão futura para todos os modelos | Séries: {total_series:,}")
    t0 = time.time()
    rows_forecast = []

    results = Parallel(n_jobs=N_NUCLEOS, backend="loky", batch_size="auto", verbose=0)(
        delayed(_worker_forecast_all_models)(
            cod_prod, regional, periodos_np, y_np, future_dates_np, horizon
        )
        for cod_prod, regional, periodos_np, y_np in tasks
    )

    for i, (rows_local, key) in enumerate(results, start=1):
        rows_forecast.extend(rows_local)

        if i == 1 or i % PRINT_EVERY == 0 or i == total_series:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0.0
            eta = (total_series - i) / rate if rate > 0 else float("inf")
            print(
                f"   ▶️ Forecast série {i}/{total_series} | {key} | "
                f"Decorrido: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min"
            )

    fato_previsao_modelo = pd.DataFrame(
        rows_forecast,
        columns=["COD_PROD", "REGIONAL", "PERIODO", "MODELO", "MODELO_EXECUTADO", "VOL_PREV"],
    )
    fato_previsao_modelo["ID_SERIE"] = fato_previsao_modelo.set_index(["COD_PROD", "REGIONAL"]).index.map(serie_map).astype(np.int64)
    fato_previsao_modelo["ID_MODELO"] = fato_previsao_modelo["MODELO"].map(MODEL_ID_MAP).astype(np.int16)
    fato_previsao_modelo = fato_previsao_modelo[
        ["ID_SERIE", "PERIODO", "ID_MODELO", "VOL_PREV", "MODELO_EXECUTADO"]
    ].sort_values(["ID_SERIE", "ID_MODELO", "PERIODO"]).reset_index(drop=True)
    print(f"🔮 FATO_PREVISAO_MODELO pronta | Linhas: {len(fato_previsao_modelo):,}")

    # ============================================================
    # 6) MÉTRICAS POR SÉRIE/MODELO (OPCIONAL)
    # ============================================================
    if CALCULAR_METRICAS:
        print("🧪 Calculando métricas por série/modelo...")
        t1 = time.time()
        rows_metricas = []

        results_metricas = Parallel(n_jobs=N_NUCLEOS, backend="loky", batch_size="auto", verbose=0)(
            delayed(_worker_metricas_all_models)(
                cod_prod, regional, periodos_np, y_np, MIN_TREINO, STEP_BACKTEST
            )
            for cod_prod, regional, periodos_np, y_np in tasks
        )

        for i, (rows_local, key) in enumerate(results_metricas, start=1):
            rows_metricas.extend(rows_local)

            if i == 1 or i % PRINT_EVERY == 0 or i == total_series:
                elapsed = time.time() - t1
                rate = i / elapsed if elapsed > 0 else 0.0
                eta = (total_series - i) / rate if rate > 0 else float("inf")
                print(
                    f"   ▶️ Métricas série {i}/{total_series} | {key} | "
                    f"Decorrido: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min"
                )

        fato_metricas_modelo = pd.DataFrame(
            rows_metricas,
            columns=[
                "COD_PROD",
                "REGIONAL",
                "MODELO",
                "MODELO_EXECUTADO",
                "VALOR_METRICA",
                "MAPE_SKU",
                "QTD_PONTOS_BACKTEST",
            ],
        )
        fato_metricas_modelo["ID_SERIE"] = fato_metricas_modelo.set_index(["COD_PROD", "REGIONAL"]).index.map(serie_map).astype(np.int64)
        fato_metricas_modelo["ID_MODELO"] = fato_metricas_modelo["MODELO"].map(MODEL_ID_MAP).astype(np.int16)
        fato_metricas_modelo["METRICA_USADA"] = METRICA_USADA
        fato_metricas_modelo = fato_metricas_modelo[
            [
                "ID_SERIE",
                "ID_MODELO",
                "VALOR_METRICA",
                "MAPE_SKU",
                "QTD_PONTOS_BACKTEST",
                "METRICA_USADA",
                "MODELO_EXECUTADO",
            ]
        ].sort_values(["ID_SERIE", "ID_MODELO"]).reset_index(drop=True)
        print(f"📐 FATO_METRICAS_MODELO pronta | Linhas: {len(fato_metricas_modelo):,}")
    else:
        fato_metricas_modelo = pd.DataFrame(
            columns=[
                "ID_SERIE",
                "ID_MODELO",
                "VALOR_METRICA",
                "MAPE_SKU",
                "QTD_PONTOS_BACKTEST",
                "METRICA_USADA",
                "MODELO_EXECUTADO",
            ]
        )
        print("⏭️ Métricas desabilitadas (CALCULAR_METRICAS=False).")

    # ============================================================
    # 7) SALVAR PARQUETS BASE
    # ============================================================
    out_dir = pasta_staging_parquet / "FORECAST_BI"
    out_dir.mkdir(parents=True, exist_ok=True)

    dim_serie.to_parquet(out_dir / "DIM_SERIE.parquet", index=False)
    dim_modelo.to_parquet(out_dir / "DIM_MODELO.parquet", index=False)
    fato_historico.to_parquet(out_dir / "FATO_HISTORICO.parquet", index=False)
    fato_previsao_modelo.to_parquet(out_dir / "FATO_PREVISAO_MODELO.parquet", index=False)
    fato_metricas_modelo.to_parquet(out_dir / "FATO_METRICAS_MODELO.parquet", index=False)

    print("✅ Parquets base salvos com sucesso:")
    print(f"   - {out_dir / 'DIM_SERIE.parquet'}")
    print(f"   - {out_dir / 'DIM_MODELO.parquet'}")
    print(f"   - {out_dir / 'FATO_HISTORICO.parquet'}")
    print(f"   - {out_dir / 'FATO_PREVISAO_MODELO.parquet'}")
    print(f"   - {out_dir / 'FATO_METRICAS_MODELO.parquet'}")

    # ============================================================
    # 8) ENRIQUECIMENTOS PÓS-SALVAMENTO
    # ============================================================
    enriquecer_dim_serie(out_dir)
    enriquecer_fatos_com_qtd(out_dir)

    print("🏁 Processamento concluído.")

    return {
        "dim_serie": dim_serie,
        "dim_modelo": dim_modelo,
        "fato_historico": fato_historico,
        "fato_previsao_modelo": fato_previsao_modelo,
        "fato_metricas_modelo": fato_metricas_modelo,
    }


# ============================================================
# EXECUÇÃO
# ============================================================
if __name__ == "__main__":
    try:
        timer.iniciar()
    except Exception:
        pass

    try:
        resultados = main()
    finally:
        try:
            timer.finalizar()
        except Exception:
            pass

# %%
timer.finalizar()
print("🎯 Processo concluído com sucesso!")


