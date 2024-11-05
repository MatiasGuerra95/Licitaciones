# Proyecto de Ranking de Licitaciones

Este proyecto tiene como objetivo automatizar el proceso de obtención, filtrado y clasificación de licitaciones, generando un ranking personalizado en una hoja de Google Sheets. El proceso incluye descargar archivos de licitaciones, aplicar filtros basados en criterios definidos y subir los resultados a Google Sheets.

## Tabla de Contenidos
- [Características](#características)
- [Requisitos](#requisitos)
- [Configuración](#configuración)
- [Uso](#uso)
- [Detalles del flujo de trabajo de GitHub Actions](#detalles-del-flujo-de-trabajo-de-github-actions)
- [Mantenimiento y logs](#mantenimiento-y-logs)

## Características
- Descarga automática de licitaciones desde URLs específicas.
- Procesamiento y limpieza de datos de licitaciones en formato CSV.
- Filtrado de licitaciones según criterios como rubros, productos, palabras clave y otros factores.
- Generación de un ranking ponderado y clasificación de las mejores 100 licitaciones.
- Integración con Google Sheets para almacenar los resultados.
- Automatización mediante GitHub Actions para ejecutar el script de procesamiento al presionar un botón en Google Sheets.

## Requisitos

- **Python 3.12** o superior.
- **Bibliotecas de Python** listadas en `requirements.txt`.
- **Credenciales de API de Google** para acceder a Google Sheets y Google Drive.
- **Token personal de GitHub** para disparar el flujo de trabajo de GitHub Actions desde Google Apps Script (GAS).
  
## Configuración

### Paso 1: Configuración de Google Sheets
1. Crea una hoja de Google Sheets con el ID especificado en el código.
2. Organiza las hojas (pestañas) de Google Sheets en el orden siguiente:
   - Hoja 1: Configuración inicial y valores (como fechas y ponderaciones).
   - Hoja 2: Ranking final.
   - Hoja 3: Rubros y productos.
   - Hoja 4: Clientes vigentes y no vigentes
   - Hoja 5: Montos 
   - Hoja 6: Lista de licitaciones seleccionadas para excluir.
   - Hoja 7: Licitaciones activas y duplicadas.
   - Hoja 10: Ranking no relativo.

### Paso 2: Configuración de credenciales de Google
1. Crea un proyecto en la [Consola de Google Cloud](https://console.cloud.google.com/).
2. Habilita las API de Google Sheets y Google Drive.
3. Genera una cuenta de servicio y descarga el archivo de credenciales JSON.
4. Guarda el contenido del JSON en un secreto de GitHub llamado `GOOGLE_APPLICATION_CREDENTIALS_JSON`.

### Paso 3: Configuración del flujo de trabajo de GitHub Actions
1. Añade el archivo `licitaciones.yml` en el directorio `.github/workflows/`.
2. Configura las credenciales en GitHub Actions como secretos:
   - `GOOGLE_APPLICATION_CREDENTIALS_JSON` con el contenido del archivo JSON de la cuenta de servicio.
   - `GITHUB_TOKEN` para autenticar las acciones de GitHub.

## Uso

### Ejecución manual
1. Ejecuta el script `your_script.py` localmente:
   ```bash
   python your_script.py
Esto descargará, procesará y subirá los datos a Google Sheets.

### Ejecución mediante GitHub Actions
1. Usa el botón de "Generar informe" en Google Sheets, el cual invoca un script de Google Apps Script que dispara el flujo de trabajo de GitHub Actions.
2. GitHub Actions ejecutará el script your_script.py y subirá los resultados a Google Sheets.

### Detalles del flujo de trabajo de GitHub Actions

Archivo licitaciones.yml
El archivo de flujo de trabajo en .github/workflows/licitaciones.yml está configurado para:

- Descargar el código del repositorio.
- Configurar Python e instalar las dependencias.
- Ejecutar el script de licitaciones con las credenciales necesarias.
- Subir el archivo de log para la revisión de errores.

Ejemplo de archivo de flujo de trabajo (licitaciones.yml):

name: Crear Ranking de Licitaciones

on:
  workflow_dispatch:
  repository_dispatch:

jobs:
  run_script:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Instalar dependencias
        run: |
          python -m pip install --upgrade pip 
          pip install -r requirements.txt

      - name: Correr Script Licitaciones
        env:
          GOOGLE_APPLICATION_CREDENTIALS_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS_JSON }}
        run: python your_script.py

      - name: Guardar archivo de log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: logs
          path: your_script.log

      - name: Output Success Message
        run: echo "Script ejecutado correctamente en GitHub Actions."

### Mantenimiento y Logs

1. El archivo your_script.log se genera después de cada ejecución del flujo de trabajo.
2. Puedes revisar los logs en la sección Artifacts en GitHub Actions para obtener información de depuración en caso de errores.

### Contacto

Para cualquier duda o sugerencia, contacta con el desarrollador del proyecto.


Este archivo `README.md` debe proporcionarte una estructura clara y detallada del proyecto. Puedes ajustar cualquier detalle adicional que sea específico para tu caso o según los cambios que vayas realizando en el proyecto.
