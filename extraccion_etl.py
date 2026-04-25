"""
ETL - FASE DE EXTRACCIÓN
========================
Extrae todas las tablas de la base de datos PostgreSQL
y las guarda como archivos CSV listos para la fase de transformación.

Dependencias:
    pip install sqlalchemy psycopg2-binary pandas python-dotenv
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

load_dotenv()  # Carga variables desde archivo .env

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "nombre_bd"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Carpeta donde se guardarán los CSVs extraídos
OUTPUT_DIR = os.getenv("ETL_OUTPUT_DIR", "datos_extraidos")

# Tablas a extraer — en orden respetando dependencias (clientes primero)
TABLAS = [
    "clientes",
    "datos_personales",
    "datos_negocio",
    "open_data",
    "cuentas",
    "tarjetas",
    "transacciones",
    "pagos",
    "cobros",
    "prestamos",
    "financiaciones",
    "seguros",
    "metas_ahorro",
    "notificaciones",
    "auditoria_comisiones",
]

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"extraccion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────

def crear_engine() -> create_engine:
    """Crea y verifica la conexión a PostgreSQL."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url, pool_pre_ping=True)

    # Verificar que la conexión funcione antes de continuar
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("Conexión a la base de datos establecida correctamente.")
    return engine


# ─────────────────────────────────────────────
# EXTRACCIÓN
# ─────────────────────────────────────────────

def extraer_tabla(engine, tabla: str, output_dir: str) -> dict:
    """
    Extrae una tabla completa y la guarda como CSV.

    Returns:
        dict con metadata de la extracción (tabla, filas, archivo, estatus)
    """
    resultado = {"tabla": tabla, "filas": 0, "archivo": None, "estatus": "error"}

    try:
        log.info(f"  → Extrayendo tabla: {tabla}")

        df = pd.read_sql_table(tabla, con=engine, schema="public")

        ruta_csv = os.path.join(output_dir, f"{tabla}.csv")
        df.to_csv(
            ruta_csv,
            index=False,
            encoding="utf-8",
            date_format="%Y-%m-%d",
        )

        resultado["filas"]   = len(df)
        resultado["archivo"] = ruta_csv
        resultado["estatus"] = "ok"
        log.info(f" {len(df):,} filas {ruta_csv}")

    except Exception as e:
        log.error(f" Error en tabla '{tabla}': {e}")

    return resultado


def ejecutar_extraccion():
    """Punto de entrada principal del proceso de extracción."""
    inicio = datetime.now()
    log.info("=" * 55)
    log.info("  ETL — INICIO DE EXTRACCIÓN")
    log.info(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # Crear carpeta de salida si no existe
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info(f" Carpeta de salida: {OUTPUT_DIR}/")

    # Conectar
    try:
        engine = crear_engine()
    except Exception as e:
        log.critical(f"No se pudo conectar a la BD: {e}")
        return

    # Extraer cada tabla
    resumen = []
    for tabla in TABLAS:
        res = extraer_tabla(engine, tabla, OUTPUT_DIR)
        resumen.append(res)

    engine.dispose()

    # ── Resumen final ──────────────────────────────
    fin = datetime.now()
    duracion = (fin - inicio).total_seconds()

    exitosas = [r for r in resumen if r["estatus"] == "ok"]
    fallidas  = [r for r in resumen if r["estatus"] == "error"]
    total_filas = sum(r["filas"] for r in exitosas)

    log.info("=" * 55)
    log.info("  RESUMEN DE EXTRACCIÓN")
    log.info("=" * 55)
    log.info(f"  Tablas extraídas : {len(exitosas)} / {len(TABLAS)}")
    log.info(f"  Total de filas   : {total_filas:,}")
    log.info(f"  Duración         : {duracion:.2f}s")

    if fallidas:
        log.warning(f"  Tablas con error : {[r['tabla'] for r in fallidas]}")

    log.info("=" * 55)
    log.info("  EXTRACCIÓN COMPLETADA")
    log.info("=" * 55)

    return resumen


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    ejecutar_extraccion()