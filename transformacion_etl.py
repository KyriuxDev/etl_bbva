"""
ETL - FASE DE TRANSFORMACIÓN
============================
Lee los CSVs extraídos, calcula señales de fraude por reglas
propias (sin usar es_fraude_potencial) y genera datasets analíticos.

Reglas implementadas:
    R1 — Anomalía geográfica  : distancia Haversine > 100 km vs ubicación habitual  (3 pts)
    R2 — Alta velocidad       : más de 3 transacciones en 1 hora por cliente         (2 pts)
    R3 — Monto anómalo        : Z-score > 3 desviaciones estándar del promedio       (2 pts)
    R4 — Hora inusual         : transacciones entre 01:00 y 05:00 h                  (1 pt)

    Score >= 3 → alerta de fraude

Ejecutar DESPUÉS de extraccion_etl.py:
    Mac/Linux : venv/bin/python transformacion_etl.py
    Windows   : venv\\Scripts\\python transformacion_etl.py
"""

import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

INPUT_DIR  = os.getenv("ETL_OUTPUT_DIR",    "datos_extraidos")
OUTPUT_DIR = os.getenv("ETL_TRANSFORM_DIR", "datos_transformados")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"transformacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo",    4: "Abril",
    5: "Mayo",  6: "Junio",   7: "Julio",    8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

UMBRAL_SCORE   = 3      # score mínimo para considerarse fraude (geo sola = 3 pts)
UMBRAL_GEO_KM  = 100    # km de distancia para flag geográfico
UMBRAL_VEL_TX  = 3      # número de tx en 1h para flag de velocidad
UMBRAL_ZSCORE  = 3      # desviaciones estándar para flag de monto
HORA_INICIO    = 1      # hora inicio rango nocturno (inclusive)
HORA_FIN       = 5      # hora fin rango nocturno   (inclusive)


# ─────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────

def cargar_datos(input_dir: str) -> dict:
    """Lee todos los CSVs necesarios para la transformación."""
    tablas = [
        "clientes",
        "cuentas",
        "transacciones",
        "datos_personales",     # necesario para latitud/longitud habitual
        "tipo_cambio_banxico",
    ]
    datos = {}
    for tabla in tablas:
        ruta = os.path.join(input_dir, f"{tabla}.csv")
        datos[tabla] = pd.read_csv(ruta, low_memory=False)
        log.info(f"  Cargado {tabla}: {len(datos[tabla]):,} filas")
    return datos


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def calcular_edad(fecha_serie: pd.Series) -> pd.Series:
    hoy = pd.Timestamp.now()
    return ((hoy - pd.to_datetime(fecha_serie, errors="coerce")).dt.days // 365).astype("Int64")


def segmento_edad(edad: pd.Series) -> pd.Series:
    return pd.cut(
        edad,
        bins=[0, 29, 59, 120],
        labels=["Joven (18-29)", "Adulto (30-59)", "Senior (60+)"],
    ).astype(str)


def nivel_riesgo(monto: pd.Series) -> pd.Series:
    return pd.cut(
        monto,
        bins=[0, 1000, 5000, float("inf")],
        labels=["Bajo", "Medio", "Alto"],
    ).astype(str)


def haversine_km(lat1: pd.Series, lon1: pd.Series,
                 lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    """
    Distancia en kilómetros entre dos pares de coordenadas (vectorizado).
    Usa la fórmula de Haversine sobre la esfera terrestre (R = 6,371 km).
    """
    R = 6371.0
    lat1r, lon1r = np.radians(lat1), np.radians(lon1)
    lat2r, lon2r = np.radians(lat2), np.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2) ** 2
    return (2 * R * np.arcsin(np.sqrt(a))).round(2)


# ─────────────────────────────────────────────
# REGLAS DE FRAUDE
# ─────────────────────────────────────────────

def regla_geo(tx: pd.DataFrame, dp: pd.DataFrame) -> pd.DataFrame:
    """
    R1 — Anomalía geográfica
    Calcula la distancia Haversine entre la ubicación de la transacción
    y la ubicación habitual del cliente.
    Puntaje: 3 pts si distancia > UMBRAL_GEO_KM.
    """
    log.info("  [R1] Calculando anomalías geográficas...")

    tx = tx.merge(
        dp[["id_cliente", "latitud_habitual", "longitud_habitual"]],
        on="id_cliente",
        how="left",
    )

    mask = (
        tx["latitud"].notna()
        & tx["longitud"].notna()
        & tx["latitud_habitual"].notna()
        & tx["longitud_habitual"].notna()
    )

    tx["distancia_km"] = 0.0
    tx.loc[mask, "distancia_km"] = haversine_km(
        tx.loc[mask, "latitud_habitual"],
        tx.loc[mask, "longitud_habitual"],
        tx.loc[mask, "latitud"],
        tx.loc[mask, "longitud"],
    )

    tx["f_geo"] = (tx["distancia_km"] > UMBRAL_GEO_KM).astype(int) * 3

    n = tx["f_geo"].gt(0).sum()
    log.info(f"     {n:,} transacciones con distancia > {UMBRAL_GEO_KM} km")
    return tx


def regla_velocidad(tx: pd.DataFrame) -> pd.DataFrame:
    """
    R2 — Alta velocidad
    Cuenta cuántas transacciones realizó el mismo cliente
    en la ventana de 1 hora anterior a cada transacción.
    Puntaje: 2 pts si count > UMBRAL_VEL_TX.

    Fix: usa merge por id_transaccion en lugar de asignación
    posicional (.values), que desalineaba filas al reordenar
    el groupby. include_groups=False silencia el DeprecationWarning
    de pandas >= 2.2.
    """
    log.info("  [R2] Calculando velocidad de transacciones...")

    tmp = (
        tx[["id_transaccion", "id_cliente", "fecha"]]
        .copy()
        .assign(fecha=lambda d: pd.to_datetime(d["fecha"]))
        .sort_values(["id_cliente", "fecha"])
        .set_index("fecha")
    )

    def _roll_cliente(g):
        """Devuelve DataFrame con id_transaccion y conteo rolling 1h."""
        conteo = g.index.to_series().rolling("1h").count().astype(int)
        return pd.DataFrame(
            {"id_transaccion": g["id_transaccion"].values, "tx_en_1h": conteo.values},
            index=g.index,
        )

    conteos = (
        tmp.groupby("id_cliente", group_keys=False)
        .apply(_roll_cliente, include_groups=False)
        [["id_transaccion", "tx_en_1h"]]
        .reset_index(drop=True)
    )

    tx = tx.merge(conteos, on="id_transaccion", how="left")
    tx["tx_en_1h"]    = tx["tx_en_1h"].fillna(1).astype(int)
    tx["f_velocidad"] = (tx["tx_en_1h"] > UMBRAL_VEL_TX).astype(int) * 2

    n = tx["f_velocidad"].gt(0).sum()
    log.info(f"     {n:,} transacciones con velocidad > {UMBRAL_VEL_TX} tx/h")
    return tx


def regla_monto_anomalo(tx: pd.DataFrame) -> pd.DataFrame:
    """
    R3 — Monto anómalo
    Calcula el Z-score de cada transacción respecto al
    promedio y desviación estándar del cliente.
    Puntaje: 2 pts si |Z-score| > UMBRAL_ZSCORE.
    """
    log.info("  [R3] Calculando montos anómalos (Z-score)...")

    stats = (
        tx.groupby("id_cliente")["monto"]
        .agg(avg_monto="mean", std_monto="std")
        .reset_index()
    )

    tx = tx.merge(stats, on="id_cliente", how="left")

    tx["z_score"] = (
        (tx["monto"] - tx["avg_monto"])
        / tx["std_monto"].replace(0, np.nan)
    ).fillna(0).round(2)

    tx["f_monto"] = (tx["z_score"].abs() > UMBRAL_ZSCORE).astype(int) * 2

    n = tx["f_monto"].gt(0).sum()
    log.info(f"     {n:,} transacciones con |Z-score| > {UMBRAL_ZSCORE}")
    return tx


def regla_hora(tx: pd.DataFrame) -> pd.DataFrame:
    """
    R4 — Hora inusual
    Marca transacciones realizadas entre HORA_INICIO y HORA_FIN.
    Puntaje: 1 pt.
    """
    log.info("  [R4] Calculando transacciones en hora inusual...")
    tx["f_hora"] = tx["hora"].between(HORA_INICIO, HORA_FIN).astype(int) * 1
    n = tx["f_hora"].gt(0).sum()
    log.info(f"     {n:,} transacciones entre {HORA_INICIO:02d}:00 y {HORA_FIN:02d}:00 h")
    return tx


# ─────────────────────────────────────────────
# TRANSFORMACIÓN PRINCIPAL
# ─────────────────────────────────────────────

def transformar(datos: dict) -> dict:
    log.info("  Iniciando transformaciones...")

    clientes    = datos["clientes"].copy()
    cuentas     = datos["cuentas"].copy()
    tx          = datos["transacciones"].copy()
    dp          = datos["datos_personales"].copy()
    tipo_cambio = datos["tipo_cambio_banxico"].copy()

    # Ignorar la etiqueta histórica — calculamos la nuestra
    tx = tx.drop(columns=["es_fraude_potencial"], errors="ignore")

    # ── Enriquecer clientes ───────────────────────────
    clientes["edad"]          = calcular_edad(clientes["fecha_nacimiento"])
    clientes["segmento_edad"] = segmento_edad(clientes["edad"])

    # ── Enriquecer transacciones (columnas temporales) ─
    tx["fecha"]      = pd.to_datetime(tx["fecha"], errors="coerce")
    tx["hora"]       = tx["fecha"].dt.hour
    tx["dia_semana"] = tx["fecha"].dt.day_name()
    tx["mes"]        = tx["fecha"].dt.month.map(MESES_ES)
    tx["año"]        = tx["fecha"].dt.year
    tx["año_mes"]    = tx["fecha"].dt.to_period("M").astype(str)
    tx["nivel_riesgo"] = nivel_riesgo(tx["monto"].fillna(0))

    # ── Aplicar reglas de fraude ──────────────────────
    tx = regla_geo(tx, dp)
    tx = regla_velocidad(tx)
    tx = regla_monto_anomalo(tx)
    tx = regla_hora(tx)

    # ── Score compuesto ───────────────────────────────
    tx["score_fraude"] = (
        tx["f_geo"]
        + tx["f_velocidad"]
        + tx["f_monto"]
        + tx["f_hora"]
    )

    log.info(f"  Score calculado — distribución:")
    log.info(f"     score 0   : {(tx['score_fraude'] == 0).sum():,} tx")
    log.info(f"     score 1-3 : {tx['score_fraude'].between(1,3).sum():,} tx")
    log.info(f"     score >= 4: {(tx['score_fraude'] >= UMBRAL_SCORE).sum():,} tx  ← alertas")

    # ── Tipo de cambio ────────────────────────────────
    tipo_cambio["fecha"] = pd.to_datetime(tipo_cambio["fecha"], errors="coerce")
    tipo_cambio = tipo_cambio[["fecha", "tipo_cambio_mxn_usd"]].dropna()
    tipo_cambio = tipo_cambio.rename(columns={"fecha": "fecha_dia"})
    tx["fecha_dia"] = tx["fecha"].dt.normalize()

    # ── Join principal ────────────────────────────────
    df = (
        tx
        .merge(
            clientes[[
                "id_cliente", "nombre", "apellido_paterno", "apellido_materno",
                "genero", "edad", "segmento_edad",
            ]],
            on="id_cliente", how="left",
        )
        .merge(
            cuentas[["num_cuenta", "tipo_cuenta", "saldo", "estatus"]],
            on="num_cuenta", how="left",
        )
        .merge(tipo_cambio, on="fecha_dia", how="left")
    )

    df["monto_usd"] = (df["monto"] / df["tipo_cambio_mxn_usd"]).round(2)

    # ── Dataset 1: Alertas de fraude ──────────────────
    fraude = df[df["score_fraude"] >= UMBRAL_SCORE].copy()

    alertas = fraude[[
        "id_transaccion", "fecha", "año_mes", "hora", "dia_semana", "mes", "año",
        "monto", "monto_usd", "tipo_cambio_mxn_usd", "nivel_riesgo",
        "tipo", "categoria", "comercio", "canal",
        "latitud", "longitud", "distancia_km",
        "z_score", "tx_en_1h", "score_fraude",
        "f_geo", "f_velocidad", "f_monto", "f_hora",
        "id_cliente", "nombre", "apellido_paterno", "apellido_materno",
        "genero", "edad", "segmento_edad",
        "num_cuenta", "tipo_cuenta", "saldo", "estatus",
    ]].reset_index(drop=True)

    log.info(f"  Alertas finales (score >= {UMBRAL_SCORE}): {len(alertas):,}")

    # ── Dataset 2: Fraude por categoría ──────────────
    fraude_categoria = (
        fraude.groupby("categoria", as_index=False)
        .agg(
            total_fraudes=("id_transaccion", "count"),
            monto_total=("monto", "sum"),
            monto_promedio=("monto", "mean"),
            score_promedio=("score_fraude", "mean"),
        )
        .sort_values("total_fraudes", ascending=False)
    )

    # ── Dataset 3: Fraude por canal ───────────────────
    fraude_canal = (
        fraude.groupby("canal", as_index=False)
        .agg(
            total_fraudes=("id_transaccion", "count"),
            monto_total=("monto", "sum"),
        )
        .assign(porcentaje=lambda x: (x["total_fraudes"] / len(fraude) * 100).round(2))
        .sort_values("total_fraudes", ascending=False)
    )

    # ── Dataset 4: Fraude por mes ─────────────────────
    fraude_mes = (
        fraude.groupby("año_mes", as_index=False)
        .agg(
            total_fraudes=("id_transaccion", "count"),
            monto_total=("monto", "sum"),
        )
        .sort_values("año_mes")
    )

    # ── Dataset 5: Resumen general ────────────────────
    resumen = pd.DataFrame([{
        "total_transacciones":    len(df),
        "total_alertas":          len(fraude),
        "tasa_fraude_pct":        round(len(fraude) / len(df) * 100, 4),
        "umbral_score":           UMBRAL_SCORE,
        "monto_total_fraude":     round(fraude["monto"].sum(), 2),
        "monto_total_fraude_usd": round(fraude["monto_usd"].sum(), 2),
        "monto_promedio_fraude":  round(fraude["monto"].mean(), 2),
        "monto_maximo_fraude":    round(fraude["monto"].max(), 2),
        "alertas_solo_geo":       int(fraude["f_geo"].gt(0).sum()),
        "alertas_solo_velocidad": int(fraude["f_velocidad"].gt(0).sum()),
        "alertas_solo_monto":     int(fraude["f_monto"].gt(0).sum()),
        "alertas_solo_hora":      int(fraude["f_hora"].gt(0).sum()),
    }])

    return {
        "alertas_fraude":       alertas,
        "fraude_por_categoria": fraude_categoria,
        "fraude_por_canal":     fraude_canal,
        "fraude_por_mes":       fraude_mes,
        "resumen_general":      resumen,
    }


# ─────────────────────────────────────────────
# GUARDAR
# ─────────────────────────────────────────────

def guardar_resultados(resultados: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    for nombre, df in resultados.items():
        ruta = os.path.join(output_dir, f"{nombre}.csv")
        df.to_csv(ruta, index=False, encoding="utf-8")
        log.info(f"  Guardado: {ruta} ({len(df):,} filas)")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def ejecutar_transformacion():
    inicio = datetime.now()
    log.info("=" * 55)
    log.info("  ETL — INICIO DE TRANSFORMACIÓN")
    log.info(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    try:
        datos = cargar_datos(INPUT_DIR)
    except FileNotFoundError as e:
        log.critical(f"Archivo no encontrado: {e}. Corre primero extraccion_etl.py")
        return

    resultados = transformar(datos)
    guardar_resultados(resultados, OUTPUT_DIR)

    duracion = (datetime.now() - inicio).total_seconds()
    log.info("=" * 55)
    log.info("  TRANSFORMACIÓN COMPLETADA")
    log.info(f"  Duración: {duracion:.2f}s")
    log.info("=" * 55)

    return resultados


if __name__ == "__main__":
    ejecutar_transformacion()