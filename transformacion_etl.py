"""
ETL - FASE DE TRANSFORMACIÓN
============================
Lee los CSVs extraídos, los enriquece y genera datasets
analíticos enfocados en detección de fraude.
"""

import os
import logging
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
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


def cargar_datos(input_dir: str) -> dict:
    tablas = ["clientes", "cuentas", "transacciones", "tipo_cambio_banxico"]
    datos = {}
    for tabla in tablas:
        ruta = os.path.join(input_dir, f"{tabla}.csv")
        datos[tabla] = pd.read_csv(ruta, low_memory=False)
        log.info(f"  Cargado {tabla}: {len(datos[tabla]):,} filas")
    return datos


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


def transformar(datos: dict) -> dict:
    log.info("  Iniciando transformaciones...")

    clientes   = datos["clientes"].copy()
    cuentas    = datos["cuentas"].copy()
    tx         = datos["transacciones"].copy()
    tipo_cambio = datos["tipo_cambio_banxico"].copy()

    # ── Preparar tipo de cambio para join por fecha ───
    tipo_cambio["fecha"] = pd.to_datetime(tipo_cambio["fecha"], errors="coerce")
    tipo_cambio = tipo_cambio[["fecha", "tipo_cambio_mxn_usd"]].dropna()

    # ── Enriquecer clientes ───────────────────────────
    clientes["edad"]          = calcular_edad(clientes["fecha_nacimiento"])
    clientes["segmento_edad"] = segmento_edad(clientes["edad"])

    # ── Enriquecer transacciones ──────────────────────
    tx["fecha"]       = pd.to_datetime(tx["fecha"], errors="coerce")
    tx["hora"]        = tx["fecha"].dt.hour
    tx["dia_semana"]  = tx["fecha"].dt.day_name()
    tx["mes"]         = tx["fecha"].dt.month.map(MESES_ES)
    tx["año"]         = tx["fecha"].dt.year
    tx["año_mes"]     = tx["fecha"].dt.to_period("M").astype(str)
    tx["nivel_riesgo"] = nivel_riesgo(tx["monto"].fillna(0))

    # ── Join principal ────────────────────────────────
    tx["fecha_dia"] = tx["fecha"].dt.normalize()
    tipo_cambio = tipo_cambio.rename(columns={"fecha": "fecha_dia"})

    df = (
        tx
        .merge(
            clientes[["id_cliente", "nombre", "apellido_paterno", "apellido_materno",
                       "genero", "edad", "segmento_edad"]],
            on="id_cliente", how="left",
        )
        .merge(
            cuentas[["num_cuenta", "tipo_cuenta", "saldo", "estatus"]],
            on="num_cuenta", how="left",
        )
        .merge(tipo_cambio, on="fecha_dia", how="left")
    )

    # Calcular monto en USD usando el tipo de cambio del día
    df["monto_usd"] = (df["monto"] / df["tipo_cambio_mxn_usd"]).round(2)

    # ── Dataset 1: Alertas de fraude enriquecidas ─────
    fraude = df[df["es_fraude_potencial"] == True].copy()
    alertas = fraude[[
        "id_transaccion", "fecha", "año_mes", "hora", "dia_semana", "mes", "año",
        "monto", "monto_usd", "tipo_cambio_mxn_usd", "nivel_riesgo",
        "tipo", "categoria", "comercio", "canal",
        "latitud", "longitud",
        "id_cliente", "nombre", "apellido_paterno", "apellido_materno",
        "genero", "edad", "segmento_edad",
        "num_cuenta", "tipo_cuenta", "saldo", "estatus",
    ]].reset_index(drop=True)
    log.info(f"  Alertas de fraude: {len(alertas):,}")

    # ── Dataset 2: Fraude por categoría ──────────────
    fraude_categoria = (
        fraude.groupby("categoria", as_index=False)
        .agg(total_fraudes=("id_transaccion", "count"),
             monto_total=("monto", "sum"),
             monto_promedio=("monto", "mean"))
        .sort_values("total_fraudes", ascending=False)
    )

    # ── Dataset 3: Fraude por canal ───────────────────
    fraude_canal = (
        fraude.groupby("canal", as_index=False)
        .agg(total_fraudes=("id_transaccion", "count"),
             monto_total=("monto", "sum"))
        .assign(porcentaje=lambda x: (x["total_fraudes"] / len(fraude) * 100).round(2))
        .sort_values("total_fraudes", ascending=False)
    )

    # ── Dataset 4: Fraude por mes ─────────────────────
    fraude_mes = (
        fraude.groupby("año_mes", as_index=False)
        .agg(total_fraudes=("id_transaccion", "count"),
             monto_total=("monto", "sum"))
        .sort_values("año_mes")
    )

    # ── Dataset 5: Resumen general ────────────────────
    resumen = pd.DataFrame([{
        "total_transacciones":    len(df),
        "total_fraudes":          len(fraude),
        "tasa_fraude_pct":        round(len(fraude) / len(df) * 100, 4),
        "monto_total_fraude":     round(fraude["monto"].sum(), 2),
        "monto_total_fraude_usd": round(fraude["monto_usd"].sum(), 2),
        "monto_promedio_fraude":  round(fraude["monto"].mean(), 2),
        "monto_maximo_fraude":    round(fraude["monto"].max(), 2),
    }])

    return {
        "alertas_fraude":       alertas,
        "fraude_por_categoria": fraude_categoria,
        "fraude_por_canal":     fraude_canal,
        "fraude_por_mes":       fraude_mes,
        "resumen_general":      resumen,
    }


def guardar_resultados(resultados: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    for nombre, df in resultados.items():
        ruta = os.path.join(output_dir, f"{nombre}.csv")
        df.to_csv(ruta, index=False, encoding="utf-8")
        log.info(f"  Guardado: {ruta} ({len(df):,} filas)")


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
