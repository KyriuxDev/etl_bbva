"""
setup.py — Configuración automática del entorno ETL
====================================================
Ejecutar UNA sola vez al clonar el proyecto:

    Windows : python setup.py
    Mac/Linux: python3 setup.py

Solo requiere tener Python 3.8+ instalado.
"""

import subprocess
import sys
import os
import shutil

VENV_DIR = "venv"
PYTHON = sys.executable


def step(msg: str):
    print(f"\n{'─'*50}\n  {msg}\n{'─'*50}")


def run(cmd: list, **kwargs):
    result = subprocess.run(cmd, **kwargs)
    if result.returncode != 0:
        print(f"\n❌ Falló el comando: {' '.join(cmd)}")
        sys.exit(1)


def main():
    print("\n" + "=" * 50)
    print("  ETL — CONFIGURACIÓN DEL ENTORNO")
    print("=" * 50)

    # ── 1. Verificar versión de Python ──────────────
    step("Verificando Python...")
    version = sys.version_info
    print(f"  Python {version.major}.{version.minor}.{version.micro} detectado")
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("  ❌ Se requiere Python 3.8 o superior.")
        sys.exit(1)
    print("  ✔ Versión compatible")

    # ── 2. Crear entorno virtual ─────────────────────
    step("Creando entorno virtual...")
    if os.path.exists(VENV_DIR):
        print(f"  ⚠ Ya existe '{VENV_DIR}/', se sobreescribirá.")
        shutil.rmtree(VENV_DIR)

    run([PYTHON, "-m", "venv", VENV_DIR])
    print(f"  ✔ Entorno virtual creado en '{VENV_DIR}/'")

    # ── 3. Determinar ruta del pip del venv ──────────
    if os.name == "nt":  # Windows
        pip = os.path.join(VENV_DIR, "Scripts", "pip.exe")
        python_venv = os.path.join(VENV_DIR, "Scripts", "python.exe")
    else:  # Mac / Linux
        pip = os.path.join(VENV_DIR, "bin", "pip")
        python_venv = os.path.join(VENV_DIR, "bin", "python")

    # ── 4. Actualizar pip ────────────────────────────
    step("Actualizando pip...")
    run([python_venv, "-m", "pip", "install", "--upgrade", "pip", "--quiet"])
    print("  ✔ pip actualizado")

    # ── 5. Instalar dependencias ─────────────────────
    step("Instalando dependencias...")
    run([pip, "install", "-r", "requirements.txt", "--quiet"])
    print("  ✔ Dependencias instaladas")

    # ── 6. Verificar archivo .env ────────────────────
    step("Verificando configuración...")
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            shutil.copy(".env.example", ".env")
            print("  ⚠ Se creó '.env' desde '.env.example'")
            print("  ➜ Edita '.env' con tus credenciales antes de correr el ETL")
        else:
            print("  ⚠ No se encontró '.env' ni '.env.example'")
            print("  ➜ Crea un archivo '.env' con las variables de conexión")
    else:
        print("  ✔ Archivo '.env' encontrado")

    # ── 7. Instrucciones finales ─────────────────────
    print("\n" + "=" * 50)
    print("  ✅ ENTORNO LISTO")
    print("=" * 50)

    if os.name == "nt":
        activate = r"venv\Scripts\activate"
        run_cmd  = r"venv\Scripts\python extraccion_etl.py"
    else:
        activate = "source venv/bin/activate"
        run_cmd  = "venv/bin/python extraccion_etl.py"

    print(f"""
  Para correr el ETL:

    {run_cmd}

  O activando el entorno primero:

    {activate}
    python extraccion_etl.py
""")


if __name__ == "__main__":
    main()