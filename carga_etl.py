"""
ETL - FASE DE CARGA
===================
Lee los CSVs transformados de datos_transformados/ y los carga
a PostgreSQL como tablas analíticas dentro de la base bbva_v2.

Tablas que se crean:
    etl_alertas_fraude       — alertas enriquecidas con datos de cliente
    etl_fraude_por_categoria — fraudes agrupados por categoría de comercio
    etl_fraude_por_canal     — fraudes agrupados por canal de pago
    etl_fraude_por_mes       — tendencia mensual 2022-2024
    etl_resumen_general      — KPIs globales del proceso

Ejecutar DESPUÉS de transformacion_etl.py:
    Mac/Linux : venv/bin/python carga_etl.py
    Windows   : venv\\Scripts\\python carga_etl.py
"""

import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "bbva_v2"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

INPUT_DIR = os.getenv("ETL_TRANSFORM_DIR", "datos_transformados")

# Mapeo: nombre_archivo nombre_tabla_destino
DATASETS = {
    "alertas_fraude":       "etl_alertas_fraude",
    "fraude_por_categoria": "etl_fraude_por_categoria",
    "fraude_por_canal":     "etl_fraude_por_canal",
    "fraude_por_mes":       "etl_fraude_por_mes",
    "resumen_general":      "etl_resumen_general",
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"carga_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────

def crear_engine():
    """Crea y verifica la conexión a PostgreSQL."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("Conexión a PostgreSQL establecida correctamente.")
    return engine


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def cargar_dataset(engine, nombre_archivo: str, nombre_tabla: str, input_dir: str) -> dict:
    """
    Lee un CSV transformado y lo carga a PostgreSQL.
    Usa if_exists='replace' para sobreescribir en cada ejecución.

    Returns:
        dict con metadata de la carga (tabla, filas, estatus)
    """
    resultado = {"tabla": nombre_tabla, "filas": 0, "estatus": "error"}

    try:
        ruta_csv = os.path.join(input_dir, f"{nombre_archivo}.csv")

        if not os.path.exists(ruta_csv):
            raise FileNotFoundError(f"No se encontró: {ruta_csv}")

        log.info(f"  → Cargando: {nombre_archivo}.csv → {nombre_tabla}")

        df = pd.read_csv(ruta_csv, low_memory=False)

        df.to_sql(
            name=nombre_tabla,
            con=engine,
            schema="public",
            if_exists="replace",   # reemplaza la tabla si ya existe
            index=False,
            chunksize=10_000,      # inserta en bloques para tablas grandes
            method="multi",
        )

        resultado["filas"]   = len(df)
        resultado["estatus"] = "ok"
        log.info(f"     {len(df):,} filas cargadas en '{nombre_tabla}'")

    except Exception as e:
        log.error(f"     Error al cargar '{nombre_tabla}': {e}")

    return resultado


def validar_carga(engine, tablas: list):
    """
    Ejecuta un SELECT COUNT(*) por cada tabla cargada
    para confirmar que los datos están en PostgreSQL.
    """
    log.info("─" * 55)
    log.info("  VALIDACIÓN DE CARGA")
    log.info("─" * 55)

    with engine.connect() as conn:
        for tabla in tablas:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM public.{tabla}"))
                count = result.scalar()
                log.info(f"  ✔ {tabla}: {count:,} filas en BD")
            except Exception as e:
                log.error(f"  ✘ {tabla}: {e}")


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def ejecutar_carga():
    inicio = datetime.now()
    log.info("=" * 55)
    log.info("  ETL — INICIO DE CARGA")
    log.info(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # Verificar que exista la carpeta de datos transformados
    if not os.path.exists(INPUT_DIR):
        log.critical(
            f"No se encontró la carpeta '{INPUT_DIR}/'. "
            "Ejecuta primero transformacion_etl.py"
        )
        return

    # Conectar a PostgreSQL
    try:
        engine = crear_engine()
    except Exception as e:
        log.critical(f"No se pudo conectar a la BD: {e}")
        return

    # Cargar cada dataset
    resumen = []
    for nombre_archivo, nombre_tabla in DATASETS.items():
        res = cargar_dataset(engine, nombre_archivo, nombre_tabla, INPUT_DIR)
        resumen.append(res)

    # Validar que los datos quedaron en BD
    tablas_ok = [r["tabla"] for r in resumen if r["estatus"] == "ok"]
    if tablas_ok:
        validar_carga(engine, tablas_ok)

    engine.dispose()

    # Resumen final
    fin      = datetime.now()
    duracion = (fin - inicio).total_seconds()
    exitosas = [r for r in resumen if r["estatus"] == "ok"]
    fallidas  = [r for r in resumen if r["estatus"] == "error"]
    total_filas = sum(r["filas"] for r in exitosas)

    log.info("=" * 55)
    log.info("  RESUMEN DE CARGA")
    log.info("=" * 55)
    log.info(f"  Tablas cargadas  : {len(exitosas)} / {len(DATASETS)}")
    log.info(f"  Total de filas   : {total_filas:,}")
    log.info(f"  Duración         : {duracion:.2f}s")

    if fallidas:
        log.warning(f"  Tablas con error : {[r['tabla'] for r in fallidas]}")

    log.info("=" * 55)
    log.info("  CARGA COMPLETADA")
    log.info("=" * 55)

    return resumen


if __name__ == "__main__":
    ejecutar_carga()