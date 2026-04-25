# ETL — Guía

## Primeros pasos (solo la primera vez)

### 1. Clona o descarga el proyecto
Asegúrate de tener todos estos archivos en la misma carpeta:
```
extraccion_etl.py
setup.py
requirements.txt
.env.example
README.md
```

### 2. Configura tus credenciales
Copia el archivo de ejemplo y edítalo:
```
# Windows
copy .env.example .env

# Mac / Linux
cp .env.example .env
```
Abre `.env` y rellena con los datos reales de conexión:
```
DB_HOST=<ip o hostname del servidor>
DB_PORT=5432
DB_NAME=<nombre de la base de datos>
DB_USER=<tu usuario>
DB_PASSWORD=<tu contraseña>
```

### 3. Ejecuta el setup (instala todo automáticamente)
```
# Windows
python setup.py

# Mac / Linux
python3 setup.py
```
Esto crea el entorno virtual e instala las dependencias. Solo se hace una vez.

---

## Correr el ETL

```
# Windows
venv\Scripts\python extraccion_etl.py

# Mac / Linux
venv/bin/python extraccion_etl.py
```

Los CSVs extraídos quedarán en la carpeta `datos_extraidos/`.

---

## Estructura del proyecto
```
├── extraccion_etl.py     # Script principal de extracción
├── setup.py              # Configuración automática del entorno
├── requirements.txt      # Dependencias de Python
├── .env.example          # Plantilla de configuración
├── .env                  # Tus credenciales (NO subir a git)
├── datos_extraidos/      # CSVs generados (se crea automáticamente)
└── README.md             # Este archivo
```