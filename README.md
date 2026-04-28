# ETL BBVA — Fase de Extracción

Extrae datos de **dos fuentes** y los guarda como archivos CSV listos para la fase de transformación.

| Fuente | Descripción | Registros |
|--------|-------------|-----------|
| PostgreSQL `bbva_v2` | 15 tablas del sistema bancario BBVA | ~2,041,495 filas |
| CSV externo (Banxico) | Tipo de cambio MXN/USD diario 2022–2024 | 754 registros |

---

## Requisitos

- Python 3.8+
- Acceso al contenedor PostgreSQL con la base `bbva_v2`
- El archivo `tipo_cambio_banxico.csv` ya viene incluido en el repositorio

---

## Configuración inicial (solo la primera vez)

### 1. Clona el repositorio

```bash
git clone https://github.com/KyriuxDev/etl_bbva.git
cd etl_bbva
```

### 2. Ejecuta el setup

Crea el entorno virtual e instala las dependencias automáticamente:

```bash
# Mac / Linux
python3 setup.py

# Windows
python setup.py
```

### 3. Configura las credenciales de la base de datos

```bash
# Mac / Linux
cp .env.example .env

# Windows
copy .env.example .env
```

Abre el archivo `.env` y rellena con los datos de conexión a PostgreSQL:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bbva_v2
DB_USER=<tu_usuario>
DB_PASSWORD=<tu_contraseña>
ETL_OUTPUT_DIR=datos_extraidos
```

---

## Ejecutar la extracción

```bash
# Mac / Linux
venv/bin/python extraccion_etl.py

# Windows
venv\Scripts\python extraccion_etl.py
```

El script extrae las dos fuentes en secuencia:

1. **Fuente 1 — PostgreSQL**: conecta a `bbva_v2` y exporta las 15 tablas
2. **Fuente 2 — CSV externo**: lee `tipo_cambio_banxico.csv` (Banco de México), normaliza fechas y tipos, y lo guarda junto a los demás

Al finalizar verás un resumen como este:

```
  Tablas extraídas : 16 / 15
  Total de filas   : 2,042,249
  Duración         : ~30s
  EXTRACCIÓN COMPLETADA
```

---

## Archivos generados

Todos los CSV quedan en la carpeta `datos_extraidos/`:

```
datos_extraidos/
├── clientes.csv
├── cuentas.csv
├── transacciones.csv
├── pagos.csv
├── cobros.csv
├── prestamos.csv
├── financiaciones.csv
├── seguros.csv
├── tarjetas.csv
├── metas_ahorro.csv
├── notificaciones.csv
├── datos_personales.csv
├── datos_negocio.csv
├── open_data.csv
├── auditoria_comisiones.csv
└── tipo_cambio_banxico.csv   ← fuente externa Banxico
```

---

## Estructura del proyecto

```
etl_bbva/
├── extraccion_etl.py        # Script principal de extracción (2 fuentes)
├── tipo_cambio_banxico.csv  # Fuente externa: tipo de cambio Banxico 2022-2024
├── setup.py                 # Configuración automática del entorno
├── requirements.txt         # Dependencias de Python
├── .env.example             # Plantilla de configuración
├── .env                     # Tus credenciales (NO se sube a git)
├── datos_extraidos/         # CSVs generados (se crea automáticamente)
└── README.md
```

---

## Sobre el CSV externo (Banxico)

El archivo `tipo_cambio_banxico.csv` contiene la **serie histórica diaria del tipo de cambio peso-dólar (CF373)** descargada del Sistema de Información Económica (SIE) del Banco de México.

- Período: `2022-01-03` al `2024-12-31`
- Columnas: `fecha`, `tipo_cambio_mxn_usd`, `fuente`
- Fuente oficial: [banxico.org.mx — SIE CF373](https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?sector=6&accion=consultarCuadro&idCuadro=CF373&locale=es)

Se usa en la fase de transformación para calcular el equivalente en USD de cada transacción fraudulenta.

---

## Fase de Transformación

> **Ejecutar después de** `extraccion_etl.py`. Requiere que la carpeta `datos_extraidos/` ya exista con los CSVs generados.

```bash
# Mac / Linux
venv/bin/python transformacion_etl.py

# Windows
venv\Scripts\python transformacion_etl.py
```

Lee las tablas `clientes`, `cuentas`, `transacciones` y `tipo_cambio_banxico` desde `datos_extraidos/`, aplica enriquecimiento y genera 5 datasets analíticos de fraude en `datos_transformados/`:

```
datos_transformados/
├── alertas_fraude.csv          # ~20,321 alertas con datos de cliente, cuenta y monto en USD
├── fraude_por_categoria.csv    # Fraudes agrupados por categoría de comercio (10 categorías)
├── fraude_por_canal.csv        # Fraudes por canal de pago (5 canales) con porcentaje
├── fraude_por_mes.csv          # Tendencia mensual 2022–2024 (36 meses)
└── resumen_general.csv         # KPIs: total transacciones, tasa de fraude, montos MXN y USD
```

Al finalizar verás:

```
  TRANSFORMACIÓN COMPLETADA
  Duración: ~Xs
```
