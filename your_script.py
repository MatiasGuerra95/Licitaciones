import logging
from logging.handlers import RotatingFileHandler
import json
import os
from datetime import datetime
import re
import unicodedata

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from zipfile import ZipFile
from io import BytesIO

from extractores.sicep import login_and_scrape  # Ensure this module is correctly implemented and accessible

# -------------------------- Configuration Constants --------------------------

# Logging Configuration
LOG_FILE = 'your_script.log'
LOG_MAX_BYTES = 10**6  # 1MB
LOG_BACKUP_COUNT = 5

# Google Sheets Configuration
SHEET_ID = '1EGoDJtO-b5dAGzC8LRYyZVdhHdcE2_ukgZAl-Ni9IxM'
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_JSON"

# URLs Configuration
BASE_URL = "https://transparenciachc.blob.core.windows.net/lic-da/"

# Health-Related Organizations to Exclude
SALUD_EXCLUIR = [
    'CENTRO DE SALUD', 'PREHOSPITALARIA', 'REFERENCIA DE SALUD',
    'REFERENCIAL DE SALUD', 'ONCOLOGICO', 'CESFAM', 'COMPLEJO ASISTENCIAL',
    'CONSULTORIO', 'CRS', 'HOSPITAL', 'INSTITUTO DE NEUROCIRUGÍA',
    'INSTITUTO DE SALUD PÚBLICA DE CHILE', 'INSTITUTO NACIONAL DE GERIATRIA',
    'INSTITUTO NACIONAL DE REHABILITACION', 'INSTITUTO NACIONAL DEL CANCER',
    'INSTITUTO NACIONAL DEL TORAX', 'INSTITUTO PSIQUIÁTRICO',
    'SERV NAC SALUD', 'SERV SALUD', 'SERVICIO DE SALUD',
    'SERVICIO NACIONAL DE SALUD', 'SERVICIO SALUD', 'INSTITUTO DE DESARROLLO AGROPECUARIO'
]

# Lista Negra (Blacklist) Configuration
# Assuming it's in column B starting from row 2 in Hoja 10
LISTA_NEGRA_RANGE = 'B2:B'

# Ranges Configuration
FECHAS_RANGE = 'C6:C7'
PALABRAS_CLAVE_RANGES = {
    'C': 'C27:C32',
    'F': 'F27:F35',
    'I': 'I27:I34'
}
RUBROS_RANGES = {
    'rubro1': 'C13',
    'rubro2': 'F13',
    'rubro3': 'I13'
}
PRODUCTOS_RANGES = {
    'rubro1': [f'C{row}' for row in range(14, 24)],
    'rubro2': [f'F{row}' for row in range(14, 24)],
    'rubro3': [f'I{row}' for row in range(14, 24)]
}

# Column Configuration
COLUMNAS_IMPORTANTES = [
    'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCreacion', 'FechaCierre',
    'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico',
    'Tipo', 'CantidadReclamos', 'TiempoDuracionContrato', 'Link'
]

# -------------------------- Logging Setup --------------------------

def setup_logging():
    """
    Configures logging with rotation to prevent log files from becoming too large.
    """
    handler = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging is configured and initialized.")

# -------------------------- Google Sheets Authentication --------------------------

def authenticate_google_sheets():
    """
    Authenticates with Google Sheets using service account credentials.

    Returns:
        gspread.Client: An authorized gspread client.
    """
    creds_json = os.environ.get(CREDENTIALS_ENV_VAR)
    if not creds_json:
        logging.error(f"La variable de entorno '{CREDENTIALS_ENV_VAR}' no está definida.")
        raise EnvironmentError(f"La variable de entorno '{CREDENTIALS_ENV_VAR}' no está definida.")

    try:
        creds_info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        gc = gspread.authorize(creds)
        logging.info("Autenticación con Google Sheets exitosa.")
        return gc
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON de credenciales: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al autenticar con Google Sheets: {e}", exc_info=True)
        raise

# -------------------------- Worksheet Retrieval with Retry --------------------------

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def get_worksheet_with_retry(spreadsheet, index):
    """
    Retrieves a worksheet by index with retry mechanism for handling API errors.

    Args:
        spreadsheet (gspread.Spreadsheet): The spreadsheet object.
        index (int): The index of the worksheet to retrieve.

    Returns:
        gspread.Worksheet: The retrieved worksheet.
    """
    try:
        worksheet = spreadsheet.get_worksheet(index)
        logging.info(f"Hoja {index + 1} obtenida exitosamente.")
        return worksheet
    except WorksheetNotFound as e:
        logging.error(f"Hoja con índice {index} no encontrada: {e}")
        raise
    except APIError as e:
        logging.warning(f"APIError al obtener Hoja {index + 1}: {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al obtener Hoja {index + 1}: {e}", exc_info=True)
        raise

# -------------------------- Utility Functions --------------------------

def eliminar_tildes(texto):
    """
    Removes diacritical marks from the input text.

    Args:
        texto (str): The text to process.

    Returns:
        str: The text without diacritical marks.
    """
    if texto:
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(char for char in texto if unicodedata.category(char) != 'Mn')
    return texto

def obtener_rango_hoja(worksheet, rango):
    """
    Retrieves values from a specified range in a worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to retrieve data from.
        rango (str): The range in A1 notation.

    Returns:
        list: A list of lists containing the values.
    """
    try:
        valores = worksheet.get(rango)
        logging.info(f"Valores obtenidos del rango {rango}.")
        return valores
    except Exception as e:
        logging.error(f"Error al obtener valores del rango {rango}: {e}", exc_info=True)
        raise

def obtener_palabras_clave(worksheet):
    """
    Retrieves and processes keyword phrases from specified ranges in the worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to retrieve keywords from.

    Returns:
        list: A list of processed keyword phrases.
    """
    try:
        palabras_clave = []
        for key, rango in PALABRAS_CLAVE_RANGES.items():
            valores = obtener_rango_hoja(worksheet, rango)
            palabras_clave.extend([p.lower() for fila in valores for p in fila if p])
        logging.info(f"Palabras clave obtenidas: {palabras_clave}")
        return palabras_clave
    except Exception as e:
        logging.error(f"Error al obtener palabras clave: {e}", exc_info=True)
        raise

def obtener_lista_negra(worksheet):
    """
    Retrieves the blacklist phrases from the specified range in the worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to retrieve the blacklist from.

    Returns:
        list: A list of blacklist phrases.
    """
    try:
        data_lista_negra = worksheet.get(LISTA_NEGRA_RANGE)
        lista_negra = [row[0].strip().lower() for row in data_lista_negra if row and row[0].strip()]
        logging.info(f"Lista negra obtenida: {lista_negra}")
        return lista_negra
    except Exception as e:
        logging.error(f"Error al obtener la lista negra: {e}", exc_info=True)
        raise

def obtener_rubros_y_productos(worksheet):
    """
    Retrieves rubros and their corresponding productos from the worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to retrieve rubros and productos from.

    Returns:
        dict: A dictionary mapping rubros to their list of productos.
    """
    try:
        rubros = {key: worksheet.acell(cell).value for key, cell in RUBROS_RANGES.items()}
        productos = {
            key: [worksheet.acell(cell).value for cell in cells if worksheet.acell(cell).value]
            for key, cells in PRODUCTOS_RANGES.items()
        }

        rubros_y_productos = {}
        for key, rubro in rubros.items():
            if rubro:
                productos_rubro = [producto.lower() for producto in productos[key]]
                rubros_y_productos[rubro.lower()] = productos_rubro
        logging.info(f"Rubros y productos obtenidos: {rubros_y_productos}")
        return rubros_y_productos
    except Exception as e:
        logging.error(f"Error al obtener rubros y productos: {e}", exc_info=True)
        raise

def obtener_puntaje_clientes(worksheet):
    """
    Retrieves clients and their statuses from the worksheet and assigns scores.

    Args:
        worksheet (gspread.Worksheet): The worksheet to retrieve client data from.

    Returns:
        dict: A dictionary mapping clients to their scores based on status.
    """
    try:
        clientes = worksheet.col_values(4)[3:]  # D4 onwards
        estados = worksheet.col_values(5)[3:]  # E4 onwards

        puntaje_clientes = {}
        for cliente, estado in zip(clientes, estados):
            estado_lower = estado.strip().lower()
            if estado_lower == 'vigente':
                puntaje_clientes[cliente.lower()] = 10
            elif estado_lower == 'no vigente':
                puntaje_clientes[cliente.lower()] = 5
            else:
                puntaje_clientes[cliente.lower()] = 0
        logging.info(f"Puntaje de clientes obtenidos: {puntaje_clientes}")
        return puntaje_clientes
    except Exception as e:
        logging.error(f"Error al obtener puntaje de clientes: {e}", exc_info=True)
        raise

def calcular_puntaje_palabra(nombre, descripcion, palabras_clave_set, lista_negra):
    """
    Calculates the word-based score for a given licitacion.

    Args:
        nombre (str): The name of the licitacion.
        descripcion (str): The description of the licitacion.
        palabras_clave_set (set): A set of keyword phrases.
        lista_negra (list): A list of blacklist phrases.

    Returns:
        int: The calculated word-based score.
    """
    try:
        texto = f"{nombre.lower()} {descripcion.lower()}"
        
        # Penalización específica
        if "consumo humano" in texto:
            logging.info(f"Penalización aplicada: frase 'consumo humano' encontrada en '{texto}'")
            return -10
        
        # Sumar puntos por palabras clave
        palabras_texto = set(re.findall(r'\b\w+\b', texto))
        puntaje_palabra = len(palabras_clave_set.intersection(palabras_texto)) * 10
        
        for palabra in palabras_clave_set.intersection(palabras_texto):
            logging.info(f"Puntos sumados por palabra clave: '{palabra}' en '{texto}'")
        
        return puntaje_palabra
    except Exception as e:
        logging.error(f"Error al calcular puntaje por palabra: {e}", exc_info=True)
        return 0

def calcular_puntaje_rubro(row, rubros_y_productos):
    """
    Calculates the rubro-based score for a given licitacion.

    Args:
        row (pd.Series): A row from the DataFrame representing a licitacion.
        rubros_y_productos (dict): A dictionary mapping rubros to productos.

    Returns:
        int: The calculated rubro-based score.
    """
    try:
        rubro_column = row['Rubro3'].lower() if pd.notnull(row['Rubro3']) else ''
        productos_column = row['Nombre producto genrico'].lower() if pd.notnull(row['Nombre producto genrico']) else ''
        puntaje_rubro = 0
        rubros_presentes = set()
        productos_presentes = set()

        for rubro, productos in rubros_y_productos.items():
            if rubro in rubro_column:
                rubros_presentes.add(rubro)
                for producto in productos:
                    if producto in productos_column:
                        productos_presentes.add(producto)

        puntaje_rubro += len(rubros_presentes) * 5
        puntaje_rubro += len(productos_presentes) * 10

        return puntaje_rubro
    except Exception as e:
        logging.error(f"Error al calcular puntaje por rubro: {e}", exc_info=True)
        return 0

def calcular_puntaje_monto(tipo_licitacion, tiempo_duracion_contrato):
    """
    Calculates the monto-based score for a given licitacion.

    Args:
        tipo_licitacion (str): The type of the licitacion.
        tiempo_duracion_contrato (str): The duration of the contract.

    Returns:
        float: The calculated monto-based score.
    """
    montos_por_tipo = {
        'L1': 0, 'LE': 100, 'LP': 1000, 'LQ': 2000, 'LR': 5000, 'LS': 0,
        'E2': 0, 'CO': 100, 'B2': 1000, 'H2': 2000, 'I2': 5000
    }

    try:
        tipo = tipo_licitacion.strip().upper()
        monto_base = montos_por_tipo.get(tipo, 0)

        tiempo_duracion = float(tiempo_duracion_contrato)
        if tiempo_duracion > 0:
            return monto_base / tiempo_duracion
        else:
            return 0
    except ValueError:
        return 0
    except Exception as e:
        logging.error(f"Error al calcular puntaje por monto: {e}", exc_info=True)
        return 0

def calcular_puntaje_clientes(nombre_organismo, puntaje_clientes):
    """
    Retrieves the client score based on the organismo's name.

    Args:
        nombre_organismo (str): The name of the organismo.
        puntaje_clientes (dict): A dictionary mapping clientes to their scores.

    Returns:
        int: The score assigned to the client.
    """
    try:
        return puntaje_clientes.get(nombre_organismo.lower(), 0)
    except Exception as e:
        logging.error(f"Error al calcular puntaje por clientes: {e}", exc_info=True)
        return 0

# -------------------------- Google Sheets Update with Retry --------------------------

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def actualizar_hoja(worksheet, rango, datos):
    """
    Updates a specified range in a worksheet with the provided data.

    Args:
        worksheet (gspread.Worksheet): The worksheet to update.
        rango (str): The range in A1 notation to update.
        datos (list): The data to upload.

    Raises:
        APIError: If the update fails due to an API error.
        Exception: For any other exceptions.
    """
    try:
        worksheet.update(rango, datos)
        logging.info(f"Hoja actualizada exitosamente en el rango {rango}.")
    except APIError as e:
        logging.warning(f"APIError al actualizar la Hoja en el rango {rango}: {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error al actualizar la Hoja en el rango {rango}: {e}", exc_info=True)
        raise

# -------------------------- Data Retrieval Functions --------------------------

def procesar_licitaciones(url):
    """
    Downloads and processes a ZIP file containing CSVs of licitaciones.

    Args:
        url (str): The URL to download the ZIP file from.

    Returns:
        pd.DataFrame: A concatenated DataFrame of all processed CSVs.
    """
    try:
        logging.info(f"Descargando licitaciones desde: {url}")
        response = requests.get(url)
        response.raise_for_status()
        zip_file = ZipFile(BytesIO(response.content))
        logging.info(f"Archivo ZIP descargado y abierto exitosamente desde: {url}")

        df_list = []
        for file_name in zip_file.namelist():
            if file_name.endswith('.csv'):
                logging.info(f"Procesando {file_name}...")
                try:
                    df = pd.read_csv(
                        zip_file.open(file_name),
                        encoding='ISO-8859-1',
                        sep=';',
                        on_bad_lines='skip',
                        low_memory=False
                    )
                    df_list.append(df)
                    logging.info(f"Archivo {file_name} procesado exitosamente.")
                except Exception as e:
                    logging.error(f"Error procesando el archivo {file_name}: {e}", exc_info=True)

        if df_list:
            df_concatenado = pd.concat(df_list, ignore_index=True)
            logging.info(f"Todos los archivos CSV de {url} han sido concatenados exitosamente.")
            return df_concatenado
        else:
            logging.warning(f"No se encontraron archivos CSV en {url}.")
            return pd.DataFrame()
    except requests.HTTPError as e:
        logging.error(f"Error HTTP al descargar {url}: {e}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error descargando o procesando el archivo desde {url}: {e}", exc_info=True)
        return pd.DataFrame()

def integrar_licitaciones_sicep(worksheet_sicep):
    """
    Integrates licitaciones from SICEP into the designated worksheet.

    Args:
        worksheet_sicep (gspread.Worksheet): The worksheet to upload SICEP licitaciones.

    Returns:
        pd.DataFrame: The DataFrame of SICEP licitaciones.
    """
    try:
        df_sicep = login_and_scrape().rename(columns={
            "Titulo": "Nombre",
            "Fecha de Publicacion": "FechaCreacion",
            "Fecha de Cierre": "FechaCierre",
            "Descripcion": "Descripcion",
            "Link": "Link"
        })

        # Ensure mandatory columns
        columnas_obligatorias = [
            "Link", "CodigoExterno", "Nombre", "Descripcion", "CodigoEstado",
            "NombreOrganismo", "Tipo", "CantidadReclamos", "FechaCreacion",
            "FechaCierre", "TiempoDuracionContrato", "Rubro3", "Nombre producto genrico"
        ]
        for columna in columnas_obligatorias:
            if columna not in df_sicep.columns:
                df_sicep[columna] = None

        # Convert to list of lists for Google Sheets
        data_to_upload = [df_sicep.columns.values.tolist()] + df_sicep.values.tolist()
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]

        # Clear and update the worksheet
        worksheet_sicep.clear()
        actualizar_hoja(worksheet_sicep, 'A1', data_to_upload)
        logging.info("Licitaciones de SICEP subidas exitosamente a la Hoja 11.")
        return df_sicep
    except APIError as e:
        logging.error(f"APIError al actualizar la Hoja 11: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error al integrar licitaciones de SICEP: {e}", exc_info=True)
        raise

# -------------------------- Main Processing Function --------------------------

def procesar_licitaciones_y_generar_ranking(
    worksheet_inicio,
    worksheet_ranking,
    worksheet_rubros,
    worksheet_clientes,
    worksheet_seleccion,
    worksheet_licitaciones_activas,
    worksheet_ranking_no_relativo,
    worksheet_lista_negra,
    worksheet_sicep
):
    """
    Processes licitaciones data and generates a ranking based on various criteria.

    Args:
        worksheet_inicio (gspread.Worksheet): Worksheet containing initial settings.
        worksheet_ranking (gspread.Worksheet): Worksheet to upload the final ranking.
        worksheet_rubros (gspread.Worksheet): Worksheet containing rubros data.
        worksheet_clientes (gspread.Worksheet): Worksheet containing clients data.
        worksheet_seleccion (gspread.Worksheet): Worksheet containing selected licitaciones.
        worksheet_licitaciones_activas (gspread.Worksheet): Worksheet containing active licitaciones.
        worksheet_ranking_no_relativo (gspread.Worksheet): Worksheet to upload non-relative scores.
        worksheet_lista_negra (gspread.Worksheet): Worksheet containing blacklist phrases.
        worksheet_sicep (gspread.Worksheet): Worksheet containing SICEP licitaciones.
    """
    try:
        # Extract minimum dates from Hoja 1
        valores_fechas = obtener_rango_hoja(worksheet_inicio, FECHAS_RANGE)
        fecha_min_publicacion = pd.to_datetime(valores_fechas[0][0], errors='coerce')
        fecha_min_cierre = pd.to_datetime(valores_fechas[1][0], errors='coerce')
        logging.info(f"Fecha mínima de publicación: {fecha_min_publicacion}")
        logging.info(f"Fecha mínima de cierre: {fecha_min_cierre}")

        # Determine current and previous month/year
        now = datetime.now()
        mes_actual = now.month
        año_actual = now.year

        if mes_actual == 1:
            mes_anterior = 12
            año_anterior = año_actual - 1
        else:
            mes_anterior = mes_actual - 1
            año_anterior = año_actual

        # Construct URLs
        url_mes_actual = f"{BASE_URL}{año_actual}-{mes_actual:02d}.zip"
        url_mes_anterior = f"{BASE_URL}{año_anterior}-{mes_anterior:02d}.zip"

        logging.info(f"URL del mes actual: {url_mes_actual}")
        logging.info(f"URL del mes anterior: {url_mes_anterior}")

        # Download and process licitaciones
        df_mes_actual = procesar_licitaciones(url_mes_actual)
        df_mes_anterior = procesar_licitaciones(url_mes_anterior)

        # Integrate SICEP licitaciones
        df_sicep = integrar_licitaciones_sicep(worksheet_sicep)

        # Concatenate all licitaciones
        df_licitaciones = pd.concat([df_mes_actual, df_mes_anterior, df_sicep], ignore_index=True)
        logging.info(f"Total de licitaciones después de concatenar: {len(df_licitaciones)}")

        # Remove diacritics and convert to lowercase
        df_licitaciones['Nombre'] = df_licitaciones['Nombre'].apply(lambda x: eliminar_tildes(x.lower()) if isinstance(x, str) else x)
        df_licitaciones['Descripcion'] = df_licitaciones['Descripcion'].apply(lambda x: eliminar_tildes(x.lower()) if isinstance(x, str) else x)

        # Filter licitaciones with 'CodigoEstado' = 5
        if 'CodigoEstado' in df_licitaciones.columns:
            df_licitaciones = df_licitaciones[df_licitaciones['CodigoEstado'] == 5]
            logging.info(f"Filtradas licitaciones con 'CodigoEstado' = 5. Total: {len(df_licitaciones)}")
        else:
            df_licitaciones = pd.DataFrame()
            logging.warning("'CodigoEstado' no está en las columnas. Se creó un DataFrame vacío.")

        # Select important columns
        df_licitaciones = df_licitaciones[df_licitaciones.columns.intersection(COLUMNAS_IMPORTANTES)]

        # Add missing columns with None
        for columna in COLUMNAS_IMPORTANTES:
            if columna not in df_licitaciones.columns:
                df_licitaciones[columna] = None

        logging.info(f"Seleccionadas columnas importantes. Total de licitaciones: {len(df_licitaciones)}")

        # Convert date columns
        for col in ['FechaCreacion', 'FechaCierre']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = pd.to_datetime(df_licitaciones[col], errors='coerce')
        
        df_licitaciones.dropna(subset=['FechaCreacion', 'FechaCierre'], inplace=True)
        logging.info("Fechas convertidas y filas con fechas inválidas eliminadas.")

        # Apply minimum date filters
        df_licitaciones = df_licitaciones[df_licitaciones['FechaCreacion'] >= fecha_min_publicacion]
        logging.info(f"Fechas después del filtro de publicación: {df_licitaciones['FechaCreacion'].unique()}")

        df_licitaciones = df_licitaciones[df_licitaciones['FechaCierre'] >= fecha_min_cierre]
        logging.info(f"Licitaciones filtradas: {len(df_licitaciones)} después de aplicar los filtros de publicación y cierre.")

        # Upload filtered licitaciones to Hoja 7
        if not df_licitaciones.empty:
            data_to_upload = [df_licitaciones.columns.values.tolist()] + df_licitaciones.values.tolist()
            data_to_upload = [[str(x) for x in row] for row in data_to_upload]
            try:
                worksheet_licitaciones_activas.clear()
                logging.info("Contenido de la Hoja 7 borrado exitosamente.")

                actualizar_hoja(worksheet_licitaciones_activas, 'A1', data_to_upload)
                logging.info("Datos actualizados en Google Sheets exitosamente en la Hoja 7.")
            except APIError as e:
                logging.error(f"APIError al actualizar la Hoja 7: {e}", exc_info=True)
                raise
            except Exception as e:
                logging.error(f"Error al actualizar la Hoja 7: {e}", exc_info=True)
                raise
        else:
            logging.warning("No se procesaron licitaciones para subir a Google Sheets.")

        # Eliminate selected licitaciones from Hoja 7 based on Hoja 6
        eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas)

        # Obtain necessary data for scoring
        palabras_clave = set(obtener_palabras_clave(worksheet_inicio))
        lista_negra = obtener_lista_negra(worksheet_lista_negra)
        rubros_y_productos = obtener_rubros_y_productos(worksheet_rubros)
        puntaje_clientes = obtener_puntaje_clientes(worksheet_clientes)
        ponderaciones = obtener_ponderaciones(worksheet_inicio)

        # Generate ranking
        generar_ranking(
            worksheet_ranking,
            worksheet_ranking_no_relativo,
            worksheet_licitaciones_activas,
            palabras_clave,
            lista_negra,
            rubros_y_productos,
            puntaje_clientes,
            ponderaciones
        )

    except Exception as e:
        logging.error(f"Error en procesar_licitaciones_y_generar_ranking: {e}", exc_info=True)
        raise

# -------------------------- Elimination Function --------------------------

def eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas):
    """
    Removes licitaciones from Hoja 7 based on selected 'CodigoExterno' in Hoja 6.

    Args:
        worksheet_seleccion (gspread.Worksheet): Worksheet containing selected 'CodigoExterno'.
        worksheet_licitaciones_activas (gspread.Worksheet): Worksheet containing active licitaciones.
    """
    try:
        codigos_seleccionados = worksheet_seleccion.col_values(1)[3:]  # Assuming 'CodigoExterno' in first column starting from row 4
        codigos_seleccionados = set([codigo.lower() for codigo in codigos_seleccionados if codigo])
        logging.info(f"Total de 'CodigoExterno' seleccionados para eliminar: {len(codigos_seleccionados)}")

        licitaciones = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones or len(licitaciones) < 2:
            logging.warning("No hay licitaciones en la Hoja 7 para procesar.")
            return

        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Total de licitaciones en la Hoja 7 antes de filtrar: {len(df_licitaciones)}")

        df_licitaciones_filtrado = df_licitaciones[~df_licitaciones['CodigoExterno'].str.lower().isin(codigos_seleccionados)]
        logging.info(f"Total de licitaciones en la Hoja 7 después de filtrar: {len(df_licitaciones_filtrado)}")

        # Prepare data for upload
        data_to_upload = [df_licitaciones_filtrado.columns.values.tolist()] + df_licitaciones_filtrado.values.tolist()
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]

        # Update Hoja 7
        worksheet_licitaciones_activas.clear()
        logging.info("Contenido de la Hoja 7 borrado antes de actualizar con datos filtrados.")

        actualizar_hoja(worksheet_licitaciones_activas, 'A1', data_to_upload)
        logging.info(f"Se eliminaron {len(codigos_seleccionados)} licitaciones seleccionadas de la Hoja 7.")
    except APIError as e:
        logging.error(f"APIError al eliminar licitaciones seleccionadas: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error al eliminar licitaciones seleccionadas: {e}", exc_info=True)
        raise

# -------------------------- Ponderaciones Function --------------------------

def obtener_ponderaciones(worksheet_inicio):
    """
    Retrieves ponderaciones from Hoja 1.

    Args:
        worksheet_inicio (gspread.Worksheet): The worksheet containing ponderaciones.

    Returns:
        dict: A dictionary with ponderaciones.
    """
    try:
        ponderaciones_valores = obtener_rango_hoja(worksheet_inicio, 'K11:K43')
        ponderaciones = {
            'Puntaje Rubro': float(ponderaciones_valores[0][0].strip('%')) / 100,
            'Puntaje Palabra': float(ponderaciones_valores[14][0].strip('%')) / 100,
            'Puntaje Clientes': float(ponderaciones_valores[28][0].strip('%')) / 100,
            'Puntaje Monto': float(ponderaciones_valores[32][0].strip('%')) / 100
        }
        logging.info(f"Ponderaciones obtenidas: {ponderaciones}")
        return ponderaciones
    except Exception as e:
        logging.error(f"Error al obtener ponderaciones: {e}", exc_info=True)
        raise

# -------------------------- Ranking Generation Function --------------------------

def generar_ranking(
    worksheet_ranking,
    worksheet_ranking_no_relativo,
    worksheet_licitaciones_activas,
    palabras_clave_set,
    lista_negra,
    rubros_y_productos,
    puntaje_clientes,
    ponderaciones
):
    """
    Generates the ranking of licitaciones and uploads the results to Google Sheets.

    Args:
        worksheet_ranking (gspread.Worksheet): Worksheet to upload the final ranking.
        worksheet_ranking_no_relativo (gspread.Worksheet): Worksheet to upload non-relative scores.
        worksheet_licitaciones_activas (gspread.Worksheet): Worksheet containing active licitaciones.
        palabras_clave_set (set): A set of keyword phrases.
        lista_negra (list): A list of blacklist phrases.
        rubros_y_productos (dict): A dictionary mapping rubros to productos.
        puntaje_clientes (dict): A dictionary mapping clientes to scores.
        ponderaciones (dict): A dictionary containing ponderaciones.
    """
    try:
        # Load licitaciones from Hoja 7
        licitaciones = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones or len(licitaciones) < 2:
            logging.warning("No hay licitaciones en la Hoja 7 para procesar.")
            return

        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Licitaciones cargadas desde la Hoja 7. Total: {len(df_licitaciones)}")

        # Filter 'TiempoDuracionContrato' != 0
        df_licitaciones = df_licitaciones[df_licitaciones['TiempoDuracionContrato'] != '0']
        logging.info(f"Filtradas licitaciones con 'TiempoDuracionContrato' != 0. Total: {len(df_licitaciones)}")

        # Exclude health-related organizations
        regex_excluir = re.compile('|'.join(SALUD_EXCLUIR), re.IGNORECASE)
        df_licitaciones = df_licitaciones[~df_licitaciones['NombreOrganismo'].str.contains(regex_excluir, na=False)]
        logging.info(f"Filtradas licitaciones relacionadas con salud. Total: {len(df_licitaciones)}")

        # Group by 'CodigoExterno'
        df_licitaciones_agrupado = df_licitaciones.groupby('CodigoExterno').agg({
            'Nombre': 'first',
            'NombreOrganismo': 'first',
            'Link': 'first',
            'Rubro3': lambda x: ' '.join(x),
            'Nombre producto genrico': lambda x: ' '.join(x),
            'Tipo': 'first',
            'CantidadReclamos': 'first',
            'Descripcion': 'first',
            'TiempoDuracionContrato': 'first'
        }).reset_index()
        logging.info(f"Licitaciones agrupadas por 'CodigoExterno'. Total: {len(df_licitaciones_agrupado)}")

        # Calculate scores
        df_licitaciones_agrupado['Puntaje Palabra'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_palabra(
                row['Nombre'], row['Descripcion'], palabras_clave_set, lista_negra
            ),
            axis=1
        )
        logging.info("Puntaje por palabras clave calculado.")

        df_licitaciones_agrupado['Puntaje Rubro'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_rubro(row, rubros_y_productos),
            axis=1
        )
        logging.info("Puntaje por rubros calculado.")

        df_licitaciones_agrupado['Puntaje Monto'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_monto(row['Tipo'], row['TiempoDuracionContrato']),
            axis=1
        )
        logging.info("Puntaje por monto calculado.")

        df_licitaciones_agrupado['Puntaje Clientes'] = df_licitaciones_agrupado['NombreOrganismo'].apply(
            lambda cliente: calcular_puntaje_clientes(cliente, puntaje_clientes)
        )
        logging.info("Puntaje por clientes calculado.")

        # Calculate total score
        df_licitaciones_agrupado['Puntaje Total'] = (
            df_licitaciones_agrupado['Puntaje Rubro'] +
            df_licitaciones_agrupado['Puntaje Palabra'] +
            df_licitaciones_agrupado['Puntaje Monto'] +
            df_licitaciones_agrupado['Puntaje Clientes']
        )
        logging.info("Puntaje total calculado.")

        # Save non-relative scores to Hoja 8
        df_no_relativos = df_licitaciones_agrupado[
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 'Puntaje Rubro', 
             'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ]

        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [[str(x) for x in row] for row in data_no_relativos]

        actualizar_hoja(worksheet_ranking_no_relativo, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 8 exitosamente.")

        # Select Top 100 licitaciones
        df_top_100 = df_licitaciones_agrupado.sort_values(
            by=['Puntaje Rubro', 'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes'], 
            ascending=False
        ).head(100)
        logging.info("Top 100 licitaciones seleccionadas.")

        # Calculate totals for each criterion
        total_rubro = df_top_100['Puntaje Rubro'].sum()
        total_palabra = df_top_100['Puntaje Palabra'].sum()
        total_monto = df_top_100['Puntaje Monto'].sum()
        total_clientes = df_top_100['Puntaje Clientes'].sum()
        logging.info("Totales calculados para cada criterio dentro del Top 100.")

        # Adjust relative scores to sum to 100
        df_top_100['Puntaje Relativo Rubro'] = (df_top_100['Puntaje Rubro'] / total_rubro * 100) if total_rubro > 0 else 0
        df_top_100['Puntaje Relativo Palabra'] = (df_top_100['Puntaje Palabra'] / total_palabra * 100) if total_palabra > 0 else 0
        df_top_100['Puntaje Relativo Monto'] = (df_top_100['Puntaje Monto'] / total_monto * 100) if total_monto > 0 else 0
        df_top_100['Puntaje Relativo Clientes'] = (df_top_100['Puntaje Clientes'] / total_clientes * 100) if total_clientes > 0 else 0
        logging.info("Puntajes relativos ajustados para que sumen 100.")

        # Calculate 'Puntaje Total SUMAPRODUCTO'
        df_top_100['Puntaje Total SUMAPRODUCTO'] = (
            df_top_100['Puntaje Relativo Rubro'] * ponderaciones['Puntaje Rubro'] +
            df_top_100['Puntaje Relativo Palabra'] * ponderaciones['Puntaje Palabra'] +
            df_top_100['Puntaje Relativo Monto'] * ponderaciones['Puntaje Monto'] +
            df_top_100['Puntaje Relativo Clientes'] * ponderaciones['Puntaje Clientes']
        )
        logging.info("Puntaje Total SUMAPRODUCTO calculado.")

        # Sort Top 100 by 'Puntaje Total SUMAPRODUCTO'
        df_top_100 = df_top_100.sort_values(by='Puntaje Total SUMAPRODUCTO', ascending=False)
        logging.info("Top 100 licitaciones ordenadas por 'Puntaje Total SUMAPRODUCTO'.")

        # Create final DataFrame for Hoja 2
        df_top_100['#'] = range(1, len(df_top_100) + 1)
        df_top_100 = df_top_100.rename(columns={
            'Puntaje Relativo Rubro': 'Rubro',
            'Puntaje Relativo Palabra': 'Palabra',
            'Puntaje Relativo Monto': 'Monto',
            'Puntaje Relativo Clientes': 'Clientes',
            'Puntaje Total SUMAPRODUCTO': 'Puntaje Final'
        })

        df_final = df_top_100[[
            '#', 'CodigoExterno', 'Nombre', 'NombreOrganismo', 'Link', 
            'Rubro', 'Palabra', 'Monto', 'Clientes', 'Puntaje Final'
        ]]

        # Ensure correct decimal formatting
        df_final['Palabra'] = df_final['Palabra'].astype(float).round(2)
        df_final['Monto'] = df_final['Monto'].astype(float).round(2)
        df_final['Puntaje Final'] = df_final['Puntaje Final'].astype(float).round(2)

        data_final = [df_final.columns.values.tolist()] + df_final.values.tolist()
        data_final = [[str(x) if isinstance(x, str) else x for x in row] for row in data_final]

        # Preserve the value of A1 in Hoja 2
        nombre_a1 = worksheet_ranking.acell('A1').value if worksheet_ranking.acell('A1').value else ""

        # Clear Hoja 2 and restore A1
        worksheet_ranking.clear()
        worksheet_ranking.update('A1', [[nombre_a1]])
        logging.info("Hoja 2 limpiada y A1 restaurado.")

        # Upload the final ranking starting from A3
        actualizar_hoja(worksheet_ranking, 'A3', data_final)
        logging.info("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")
    except Exception as e:
        logging.error(f"Error al generar el ranking: {e}", exc_info=True)
        raise

# -------------------------- Main Function --------------------------

def main():
    """
    The main function orchestrating the entire processing workflow.
    """
    try:
        # Setup logging
        setup_logging()

        # Authenticate with Google Sheets
        gc = authenticate_google_sheets()

        # Open the spreadsheet
        try:
            sh = gc.open_by_key(SHEET_ID)
            logging.info(f"Spreadsheet con ID {SHEET_ID} abierto exitosamente.")
        except SpreadsheetNotFound as e:
            logging.error(f"Spreadsheet con ID {SHEET_ID} no encontrado: {e}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Error al abrir Spreadsheet: {e}", exc_info=True)
            raise

        # Retrieve worksheets
        try:
            worksheet_inicio = get_worksheet_with_retry(sh, 0)  # Hoja Inicio
            worksheet_ranking = get_worksheet_with_retry(sh, 1)  # Hoja Ranking
            worksheet_rubros = get_worksheet_with_retry(sh, 2)  # Hoja Rubros
            worksheet_clientes = get_worksheet_with_retry(sh, 3)  # Hoja Clientes
            worksheet_seleccion = get_worksheet_with_retry(sh, 5)  # Hoja Seleccion
            worksheet_licitaciones_activas = get_worksheet_with_retry(sh, 6)  # Hoja Licitaciones Activas y Duplicadas
            worksheet_ranking_no_relativo = get_worksheet_with_retry(sh, 7)  # Hoja Ranking no relativo
            worksheet_lista_negra = get_worksheet_with_retry(sh, 9)  # Hoja Lista Negra Palabras
            worksheet_sicep = get_worksheet_with_retry(sh, 10)  # Hoja Licitaciones Sicep
        except Exception as e:
            logging.error(f"Error al obtener una o más hojas: {e}", exc_info=True)
            raise

        # Execute main processing
        procesar_licitaciones_y_generar_ranking(
            worksheet_inicio,
            worksheet_ranking,
            worksheet_rubros,
            worksheet_clientes,
            worksheet_seleccion,
            worksheet_licitaciones_activas,
            worksheet_ranking_no_relativo,
            worksheet_lista_negra,
            worksheet_sicep
        )

    except Exception as e:
        logging.critical(f"Script finalizado con errores: {e}", exc_info=True)
        raise

# -------------------------- Entry Point --------------------------

if __name__ == "__main__":
    main()
