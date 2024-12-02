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

from extractores.sicep import login_and_scrape  # Asegúrate de que este módulo está correctamente implementado y accesible

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
    'rubro1': [f'D{row}' for row in range(14, 24)],
    'rubro2': [f'G{row}' for row in range(14, 24)],
    'rubro3': [f'J{row}' for row in range(14, 24)]
}

# Column Configuration
COLUMNAS_IMPORTANTES = [
    'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaInicio', 'FechaCierre',
    'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico',
    'Tipo', 'CantidadReclamos', 'TiempoDuracionContrato', 'Link', 'CodigoProductoONU'
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

# -------------------------- Serialization Function --------------------------

def serialize_value(x):
    """
    Convierte objetos no serializables a un formato serializable.
    
    Args:
        x: El valor a serializar.
    
    Returns:
        El valor serializado.
    """
    if pd.isnull(x):
        return ''
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.isoformat()
    return x

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
def get_worksheet_with_retry(spreadsheet, nombre):
    """
    Retrieves a worksheet by name with retry mechanism for handling API errors.

    Args:
        spreadsheet (gspread.Spreadsheet): The spreadsheet object.
        nombre (str): The name of the worksheet to retrieve.

    Returns:
        gspread.Worksheet: The retrieved worksheet.
    """
    try:
        worksheet = spreadsheet.worksheet(nombre)
        logging.info(f"Hoja '{nombre}' obtenida exitosamente.")
        return worksheet
    except WorksheetNotFound as e:
        logging.error(f"Hoja '{nombre}' no encontrada: {e}")
        raise
    except APIError as e:
        logging.warning(f"APIError al obtener Hoja '{nombre}': {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al obtener Hoja '{nombre}': {e}", exc_info=True)
        raise

# -------------------------- Utility Functions --------------------------

def eliminar_tildes_y_normalizar(texto):
    """
    Elimina las tildes de una cadena de texto y normaliza eliminando espacios adicionales.

    Args:
        texto (str): El texto a procesar.

    Returns:
        str: El texto sin tildes, sin espacios extra y en minúsculas.
    """
    if texto and isinstance(texto, str):
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(char for char in texto if unicodedata.category(char) != 'Mn')
        texto = re.sub(r'\s+', ' ', texto)  # Eliminar espacios adicionales
        texto = texto.strip().lower()
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

def obtener_palabras_clave(worksheet_inicio):
    """
    Retrieves and processes keyword phrases from specified ranges in the worksheet.

    Args:
        worksheet_inicio (gspread.Worksheet): The worksheet to retrieve keywords from.

    Returns:
        set: A set of processed keyword phrases.
    """
    try:
        palabras_clave = []
        for key, rango in PALABRAS_CLAVE_RANGES.items():
            valores = obtener_rango_hoja(worksheet_inicio, rango)
            palabras_clave.extend([eliminar_tildes_y_normalizar(p) for fila in valores for p in fila if p])
        palabras_clave_set = set(palabras_clave)
        logging.info(f"Palabras clave obtenidas: {palabras_clave_set}")
        return palabras_clave_set
    except Exception as e:
        logging.error(f"Error al obtener palabras clave: {e}", exc_info=True)
        raise

def obtener_lista_negra(worksheet_lista_negra):
    """
    Retrieves the blacklist phrases from the specified range in the worksheet.

    Args:
        worksheet_lista_negra (gspread.Worksheet): The worksheet to retrieve the blacklist from.

    Returns:
        set: A set of blacklist phrases.
    """
    try:
        data_lista_negra = obtener_rango_hoja(worksheet_lista_negra, LISTA_NEGRA_RANGE)
        lista_negra = set([eliminar_tildes_y_normalizar(row[0]) for row in data_lista_negra if row and row[0].strip()])
        logging.info(f"Lista negra obtenida: {lista_negra}")
        return lista_negra
    except Exception as e:
        logging.error(f"Error al obtener la lista negra: {e}", exc_info=True)
        raise

def obtener_rubros_y_productos(worksheet_inicio):
    """
    Retrieves rubros and their corresponding productos from the worksheet.

    Args:
        worksheet_rubros (gspread.Worksheet): The worksheet to retrieve rubros and productos from.

    Returns:
        dict: A dictionary mapping rubros to their list of productos.
    """
    try:
        # Definir los rangos de rubros
        rubros_ranges = list(RUBROS_RANGES.values())  # ['C13', 'F13', 'I13']
        
        # Utilizar batch_get para recuperar todos los rangos de rubros de una sola vez
        valores_rubros = worksheet_inicio.batch_get(rubros_ranges)
        logging.debug(f"Valores de rubros obtenidos: {valores_rubros}")
        
        # Extraer los valores de rubros
        rubros = {
            key: valores[0][0].strip() if valores and valores[0] and valores[0][0] else None
            for key, valores in zip(RUBROS_RANGES.keys(), valores_rubros)
        }
        logging.debug(f"Rubros extraídos: {rubros}")

        # Verificar si los rubros están vacíos
        for key, rubro in rubros.items():
            if rubro is None:
                logging.warning(f"Rubro '{key}' está vacío en la celda {RUBROS_RANGES[key]}.")

        # Definir los rangos de productos
        rangos_productos = [r for key in PRODUCTOS_RANGES for r in PRODUCTOS_RANGES[key]]
        
        # Utilizar batch_get para recuperar todos los rangos de productos de una sola vez
        valores_productos = worksheet_inicio.batch_get(rangos_productos)
        logging.debug(f"Valores de productos obtenidos: {valores_productos}")
        
        # Asignar productos a cada rubro
        productos = {}
        index = 0
        for key in PRODUCTOS_RANGES.keys():
            productos[key] = [
                eliminar_tildes_y_normalizar(valores_productos[index + i][0][0])
                for i in range(len(PRODUCTOS_RANGES[key]))
                if valores_productos[index + i] and valores_productos[index + i][0][0].strip()
            ]
            index += len(PRODUCTOS_RANGES[key])
        logging.debug(f"Productos asignados por rubro: {productos}")

        # Mapear rubros a productos
        rubros_y_productos = {
            eliminar_tildes_y_normalizar(rubro.lower()): productos.get(key, [])
            for key, rubro in rubros.items()
            if rubro and rubro.strip() != ""
        }

        logging.info(f"Rubros y productos obtenidos: {rubros_y_productos}")
        return rubros_y_productos

    except Exception as e:
        logging.error(f"Error al obtener rubros y productos: {e}", exc_info=True)
        raise

def obtener_puntaje_clientes(worksheet_clientes):
    """
    Retrieves clients and their statuses from the worksheet and assigns scores.

    Args:
        worksheet_clientes (gspread.Worksheet): The worksheet to retrieve client data from.

    Returns:
        dict: A dictionary mapping clients to their scores based on status.
    """
    try:
        # Recuperar todas las filas de clientes y estados de una vez
        clientes = worksheet_clientes.col_values(4)[3:]  # D4 en adelante
        estados = worksheet_clientes.col_values(5)[3:]   # E4 en adelante

        if not clientes or not estados:
            logging.warning("No se encontraron datos en las columnas de clientes o estados.")
            return {}

        if len(clientes) != len(estados):
            logging.warning("La cantidad de clientes y estados no coincide.")

        puntaje_clientes = {}
        for cliente, estado in zip(clientes, estados):
            estado_lower = estado.strip().lower()
            cliente_normalizado = eliminar_tildes_y_normalizar(cliente.lower().strip())
            if estado_lower == 'vigente':
                puntaje_clientes[cliente_normalizado] = 10
            elif estado_lower == 'no vigente':
                puntaje_clientes[cliente_normalizado] = 5
            else:
                puntaje_clientes[cliente_normalizado] = 0
        logging.info(f"Puntaje de clientes obtenidos: {puntaje_clientes}")
        return puntaje_clientes
    except Exception as e:
        logging.error(f"Error al obtener puntaje de clientes: {e}", exc_info=True)
        raise

def calcular_puntaje_palabra(row, palabras_clave_set, lista_negra):
    """
    Calculates the word-based score for a given licitacion.

    Args:
        row (pd.Series): A row from the DataFrame representing a licitacion.
        palabras_clave_set (set): A set of keyword phrases.
        lista_negra (set): A set of blacklist phrases.

    Returns:
        int: The calculated word-based score.
    """
    try:
        nombre = eliminar_tildes_y_normalizar(row['Nombre']) if pd.notnull(row['Nombre']) else ''
        descripcion = eliminar_tildes_y_normalizar(row['Descripcion']) if pd.notnull(row['Descripcion']) else ''
        puntaje_palabra = 0

        # Combinar nombre y descripción
        texto = f"{nombre} {descripcion}"

        # Tokenizar el texto
        palabras_texto = set(re.findall(r'\b\w+\b', texto))

        # Excluir palabras de la lista negra
        palabras_texto = palabras_texto - lista_negra

        # Calcular intersección con palabras clave
        palabras_encontradas = palabras_clave_set.intersection(palabras_texto)
        puntaje_palabra += len(palabras_encontradas) * 10  # +10 por cada palabra clave encontrada

        for palabra in palabras_encontradas:
            logging.info(f"Palabra clave '{palabra}' encontrada en la licitación.")

        logging.debug(f"Puntaje calculado para palabras clave: {puntaje_palabra}")
        return puntaje_palabra
    except Exception as e:
        logging.error(f"Error al calcular puntaje por palabra clave: {e}", exc_info=True)
        return 0

def calcular_puntaje_rubro(row, rubros_y_productos):
    """
    Calcula el puntaje basado en los rubros y el código de producto ONU definido.

    Args:
        row (pd.Series): Una fila del DataFrame representando una licitación.
        rubros_y_productos (dict): Diccionario que mapea rubros a listas de códigos de productos ONU.

    Returns:
        int: Puntaje calculado basado en rubros y códigos de productos ONU.
    """
    try:
        rubro_column = row.get('Rubro3', '') if pd.notnull(row.get('Rubro3', '')) else ''
        codigo_producto_column = str(row.get('CodigoProductoONU', '')).strip() if pd.notnull(row.get('CodigoProductoONU', '')) else ''
        puntaje_rubro = 0

        rubros_presentes = set()
        productos_presentes = set()

        for rubro, productos in rubros_y_productos.items():
            # Comparación parcial para el rubro
            if rubro and rubro in rubro_column:
                rubros_presentes.add(rubro)

            # Comparación exacta para los productos asociados al rubro
            if codigo_producto_column in productos:
                productos_presentes.add(codigo_producto_column)

        puntaje_rubro += len(rubros_presentes) * 5  # Puntaje por cada rubro coincidente
        puntaje_rubro += len(productos_presentes) * 10  # Puntaje por cada código de producto coincidente

        logging.debug(f"Fila evaluada: Rubros={rubros_presentes}, Productos ONU={productos_presentes}, Puntaje={puntaje_rubro}")
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
            puntaje_monto = monto_base / tiempo_duracion
            return puntaje_monto
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
        if not nombre_organismo:
            return 0
        cliente_normalizado = eliminar_tildes_y_normalizar(nombre_organismo.lower().strip())
        puntaje = puntaje_clientes.get(cliente_normalizado, 0)
        logging.info(f"Cliente '{cliente_normalizado}' tiene puntaje {puntaje}")
        return puntaje
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
    Actualiza un rango específico en una hoja con los datos proporcionados.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo a actualizar.
        rango (str): El rango en notación A1.
        datos (list): Los datos a subir.

    Raises:
        APIError: Si la actualización falla debido a un error de la API.
        Exception: Para cualquier otro error.
    """
    try:
        # Aplicar serialización a todos los elementos
        datos_serializados = [
            [serialize_value(x) for x in row]
            for row in datos
        ]

        worksheet.update(
            rango,
            datos_serializados,
            value_input_option='USER_ENTERED'  # Permite que Sheets interprete los tipos de datos correctamente
        )
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
            "Fecha de Publicacion": "FechaInicio",
            "Fecha de Cierre": "FechaCierre",
            "Descripcion": "Descripcion",
            "Link": "Link"
        })

        # Ensure mandatory columns
        columnas_obligatorias = [
            "CodigoExterno", "Nombre", "Descripcion", "CodigoEstado",
            "NombreOrganismo", "Tipo", "CantidadReclamos", "FechaInicio",
            "FechaCierre", "TiempoDuracionContrato", "Rubro3", "Nombre producto genrico", "Link"
        ]
        for columna in columnas_obligatorias:
            if columna not in df_sicep.columns:
                df_sicep[columna] = None

        # Convert to list of lists for Google Sheets
        data_to_upload = [df_sicep.columns.values.tolist()] + df_sicep.values.tolist()
        data_to_upload = [
            [serialize_value(x) for x in row]
            for row in data_to_upload
        ]

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

# -------------------------- Rastrear Licitacion Function --------------------------

def rastrear_licitacion(df, codigo_a_rastrear, etapa):
    """
    Rastrea una licitación específica y registra su estado en cada etapa.

    Args:
        df (pd.DataFrame): El DataFrame que representa las licitaciones actuales.
        codigo_a_rastrear (str): El CodigoExterno de la licitacion a rastrear.
        etapa (str): La etapa actual del procesamiento.
    """
    try:
        # Filtrar la licitación específica
        licitacion = df[df['CodigoExterno'] == codigo_a_rastrear]
        if not licitacion.empty:
            logging.info(f"Etapa: {etapa} - Licitación encontrada: {codigo_a_rastrear}")
            # Opcional: Registrar detalles adicionales si es necesario
            detalles = licitacion.to_dict('records')[0]
            logging.debug(f"Detalles de la licitación en etapa '{etapa}': {detalles}")
        else:
            logging.info(f"Etapa: {etapa} - Licitación no encontrada: {codigo_a_rastrear}")
    except KeyError:
        logging.error(f"La columna 'CodigoExterno' no existe en el DataFrame durante la etapa '{etapa}'.")
    except Exception as e:
        logging.error(f"Error al rastrear la licitación en la etapa '{etapa}': {e}", exc_info=True)

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
        # Definir el CodigoExterno a rastrear
        CODIGO_A_RASTREAR = '5482-99-LE24'  # Tu CodigoExterno específico

        # Extract minimum dates from Hoja 1
        valores_fechas = obtener_rango_hoja(worksheet_inicio, FECHAS_RANGE)
        if len(valores_fechas) < 2 or not all(valores_fechas):
            logging.error("No se pudieron obtener las fechas mínimas desde la Hoja 1.")
            raise ValueError("Fechas mínimas no encontradas.")
        
        fecha_min_publicacion = pd.to_datetime(valores_fechas[0][0], errors='coerce')
        fecha_min_cierre = pd.to_datetime(valores_fechas[1][0], errors='coerce')

        if fecha_min_publicacion is pd.NaT or fecha_min_cierre is pd.NaT:
            logging.error("Formato de fecha incorrecto en la Hoja 1.")
            raise ValueError("Fechas mínimas no válidas.")

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

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_licitaciones, CODIGO_A_RASTREAR, 'Después de Concatenar')

        # Remove diacritics and convert to lowercase
        for col in ['Nombre', 'Descripcion', 'Rubro3', 'Nombre producto genrico', 'NombreOrganismo']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = df_licitaciones[col].apply(lambda x: eliminar_tildes_y_normalizar(x) if isinstance(x, str) else x)
        
        if 'CodigoProductoONU' in df_licitaciones.columns:
            df_licitaciones['CodigoProductoONU'] = df_licitaciones['CodigoProductoONU'].apply(lambda x: eliminar_tildes_y_normalizar(str(x).split('.')[0]) if pd.notnull(x) else x)

        # Convert date columns to datetime
        for col in ['FechaInicio', 'FechaCierre']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = pd.to_datetime(df_licitaciones[col], errors='coerce')

        # Filter by minimum dates
        df_nuevas_filtradas = df_licitaciones[
            (df_licitaciones['FechaInicio'] >= fecha_min_publicacion) &
            (df_licitaciones['FechaCierre'] >= fecha_min_cierre)
        ]
        logging.info(f"Total de licitaciones después de aplicar filtros de fecha: {len(df_nuevas_filtradas)}")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_nuevas_filtradas, CODIGO_A_RASTREAR, 'Después de Filtrar por Fechas')

        if df_nuevas_filtradas.empty:
            logging.warning("No hay nuevas licitaciones que cumplan con los criterios de fecha.")
        else:
            # Clear Hoja 7 before uploading new data
            worksheet_licitaciones_activas.clear()
            logging.info("Hoja 7 (Licitaciones MP) limpiada exitosamente.")

            # Ensure all necessary columns are present
            for columna in COLUMNAS_IMPORTANTES:
                if columna not in df_nuevas_filtradas.columns:
                    df_nuevas_filtradas[columna] = None  # Assign None if column is missing

            # Order columns according to COLUMNAS_IMPORTANTES
            df_nuevas_filtradas = df_nuevas_filtradas[COLUMNAS_IMPORTANTES]

            # Convert to list of lists for Google Sheets
            data_to_upload = [df_nuevas_filtradas.columns.values.tolist()] + df_nuevas_filtradas.values.tolist()
            data_to_upload = [
                [serialize_value(x) for x in row]
                for row in data_to_upload
            ]

            # Upload the data to Hoja 7
            actualizar_hoja(worksheet_licitaciones_activas, 'A1', data_to_upload)
            logging.info("Nuevas licitaciones cargadas a la Hoja 7 (Licitaciones MP).")

        # -------------- Eliminar Licitaciones Seleccionadas --------------
        # Call the function to remove selected licitaciones after uploading new licitaciones
        eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas)

        # Re-obtain active licitaciones after elimination
        licitaciones_actualizadas = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones_actualizadas or len(licitaciones_actualizadas) < 2:
            logging.warning("No hay licitaciones activas después de la eliminación.")
            return

        df_licitaciones = pd.DataFrame(licitaciones_actualizadas[1:], columns=licitaciones_actualizadas[0])
        logging.info(f"Total de licitaciones activas después de eliminar seleccionadas: {len(df_licitaciones)}")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_licitaciones, CODIGO_A_RASTREAR, 'Después de Eliminar Seleccionadas')

        # Normalize and clean columns before processing
        for col in ['Nombre', 'Descripcion', 'Rubro3', 'Nombre producto genrico', 'NombreOrganismo']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = df_licitaciones[col].apply(lambda x: eliminar_tildes_y_normalizar(x) if isinstance(x, str) else x)
        
        if 'CodigoProductoONU' in df_licitaciones.columns:
            df_licitaciones['CodigoProductoONU'] = df_licitaciones['CodigoProductoONU'].apply(lambda x: eliminar_tildes_y_normalizar(str(x).split('.')[0]) if pd.notnull(x) else x)

        # Convert date columns to datetime
        for col in ['FechaInicio', 'FechaCierre']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = pd.to_datetime(df_licitaciones[col], errors='coerce')

        # -------------- Exclude Health-Related Organizations (Vectorized) --------------

        # Create a regex pattern for exclusion terms
        patron_exclusion = '|'.join([re.escape(termino.lower()) for termino in SALUD_EXCLUIR])

        # Normalize 'NombreOrganismo' for filtering
        df_licitaciones['NombreOrganismo_normalizado'] = df_licitaciones['NombreOrganismo'].astype(str).apply(lambda x: eliminar_tildes_y_normalizar(x))

        # Apply the filter using str.contains with case insensitivity
        df_filtrado_salud = df_licitaciones[
            df_licitaciones['NombreOrganismo_normalizado'].str.contains(patron_exclusion, case=False, na=False)
        ]
        num_filtradas_salud = len(df_filtrado_salud)
        df_licitaciones = df_licitaciones[
            ~df_licitaciones['NombreOrganismo_normalizado'].str.contains(patron_exclusion, case=False, na=False)
        ]
        logging.info(f"Total de licitaciones después de excluir organizaciones de salud: {len(df_licitaciones)}")
        logging.info(f"Total de licitaciones filtradas por salud: {num_filtradas_salud}")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_licitaciones, CODIGO_A_RASTREAR, 'Después de Excluir Salud')

        # -------------- Calcular Puntajes --------------
        palabras_clave = obtener_palabras_clave(worksheet_inicio)
        lista_negra = obtener_lista_negra(worksheet_lista_negra)
        rubros_y_productos = obtener_rubros_y_productos(worksheet_inicio)
        puntaje_clientes = obtener_puntaje_clientes(worksheet_clientes)
        ponderaciones = obtener_ponderaciones(worksheet_inicio)

        df_licitaciones['Puntaje Palabra'] = df_licitaciones.apply(
            lambda row: calcular_puntaje_palabra(row, palabras_clave, lista_negra), axis=1
        )
        df_licitaciones['Puntaje Rubro'] = df_licitaciones.apply(
            lambda row: calcular_puntaje_rubro(row, rubros_y_productos), axis=1
        )
        df_licitaciones['Puntaje Monto'] = df_licitaciones.apply(
            lambda row: calcular_puntaje_monto(row['Tipo'], row['TiempoDuracionContrato']), axis=1
        )
        df_licitaciones['Puntaje Clientes'] = df_licitaciones['NombreOrganismo'].apply(
            lambda cliente: calcular_puntaje_clientes(cliente, puntaje_clientes)
        )

        # Calcular puntaje total
        df_licitaciones['Puntaje Total'] = (
            df_licitaciones['Puntaje Rubro'] +
            df_licitaciones['Puntaje Palabra'] +
            df_licitaciones['Puntaje Monto'] +
            df_licitaciones['Puntaje Clientes']
        )
        logging.info("Puntaje total calculado.")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_licitaciones, CODIGO_A_RASTREAR, 'Después de Calcular Puntajes')

        # Eliminar duplicados basados en 'CodigoExterno', manteniendo la fila con mayor 'Puntaje Total'
        df_licitaciones_unique = df_licitaciones.sort_values(by='Puntaje Total', ascending=False).drop_duplicates(subset='CodigoExterno', keep='first')
        logging.info(f"Licitaciones después de eliminar duplicados: {len(df_licitaciones_unique)}")        

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_licitaciones_unique, CODIGO_A_RASTREAR, 'Después de Eliminar Duplicados')

        # Guardar puntajes NO relativos en Hoja 8
        df_no_relativos = df_licitaciones_unique[
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 
             'Puntaje Rubro', 'Puntaje Palabra', 
             'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ].copy()  # Asegura una copia independiente

        # Convertir a lista de listas y serializar
        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [
            [serialize_value(x) for x in row]
            for row in data_no_relativos
        ]

        # Limpiar la Hoja 8 antes de subir nuevos datos
        worksheet_ranking_no_relativo.clear()
        logging.info("Hoja 8 (Ranking no relativo) limpiada exitosamente.")        

        # Subir los datos a Google Sheets
        actualizar_hoja(worksheet_ranking_no_relativo, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 8 exitosamente.")

        # Seleccionar Top 100 licitaciones
        df_top_100 = df_licitaciones_unique.sort_values(
            by=['Puntaje Rubro', 'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes'], 
            ascending=False
        ).head(100)
        logging.info("Top 100 licitaciones seleccionadas.")

        # Calcular totales para cada criterio dentro del Top 100
        total_rubro = df_top_100['Puntaje Rubro'].sum()
        total_palabra = df_top_100['Puntaje Palabra'].sum()
        total_monto = df_top_100['Puntaje Monto'].sum()
        total_clientes = df_top_100['Puntaje Clientes'].sum()
        logging.info("Totales calculados para cada criterio dentro del Top 100.")

        # Ajustar puntajes relativos para que sumen 100
        df_top_100['Puntaje Relativo Rubro'] = df_top_100['Puntaje Rubro'].apply(
            lambda x: (x / total_rubro * 100) if total_rubro > 0 else 0
        )
        df_top_100['Puntaje Relativo Palabra'] = df_top_100['Puntaje Palabra'].apply(
            lambda x: (x / total_palabra * 100) if total_palabra > 0 else 0
        )
        df_top_100['Puntaje Relativo Monto'] = df_top_100['Puntaje Monto'].apply(
            lambda x: (x / total_monto * 100) if total_monto > 0 else 0
        )
        df_top_100['Puntaje Relativo Clientes'] = df_top_100['Puntaje Clientes'].apply(
            lambda x: (x / total_clientes * 100) if total_clientes > 0 else 0
        )
        logging.info("Puntajes relativos ajustados para que sumen 100.")

        # Calcular 'Puntaje Total SUMAPRODUCTO'
        df_top_100['Puntaje Total SUMAPRODUCTO'] = (
            df_top_100['Puntaje Relativo Rubro'] * ponderaciones.get('Puntaje Rubro', 0) +
            df_top_100['Puntaje Relativo Palabra'] * ponderaciones.get('Puntaje Palabra', 0) +
            df_top_100['Puntaje Relativo Monto'] * ponderaciones.get('Puntaje Monto', 0) +
            df_top_100['Puntaje Relativo Clientes'] * ponderaciones.get('Puntaje Clientes', 0)
        )
        logging.info("Puntaje Total SUMAPRODUCTO calculado.")

        # Ordenar Top 100 por 'Puntaje Total SUMAPRODUCTO'
        df_top_100 = df_top_100.sort_values(by='Puntaje Total SUMAPRODUCTO', ascending=False)
        logging.info("Top 100 licitaciones ordenadas por 'Puntaje Total SUMAPRODUCTO'.")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_top_100, CODIGO_A_RASTREAR, 'Después de Ordenar Top 100')

        # Crear estructura para Hoja 2
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

        # Verificar duplicados en df_final
        duplicate_codes_final = df_final[df_final.duplicated(subset='CodigoExterno', keep=False)]
        num_duplicates_final = len(duplicate_codes_final)
        if num_duplicates_final > 0:
            duplicate_codes_list = duplicate_codes_final['CodigoExterno'].unique().tolist()
            logging.warning(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
            print(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
        else:
            logging.info("No hay licitaciones duplicadas en el Top 100.")
            print("No hay licitaciones duplicadas en el Top 100.")        

        # Asegurar formato correcto de decimales
        for col in ['Palabra', 'Monto', 'Puntaje Final']:
            df_final[col] = df_final[col].astype(float).round(2)

        # Convert to list of lists and serialize
        data_final = [df_final.columns.values.tolist()] + df_final.values.tolist()
        data_final = [
            [serialize_value(x) for x in row]
            for row in data_final
        ]

        # Preserve the value of A1 in Hoja 2
        nombre_a1 = worksheet_ranking.acell('A1').value if worksheet_ranking.acell('A1').value else ""

        # Clear Hoja 2 and restore A1
        worksheet_ranking.clear()
        worksheet_ranking.update('A1', [[nombre_a1]], value_input_option='USER_ENTERED')
        logging.info("Hoja 2 (Ranking) limpiada y A1 restaurado.")

        # Upload the final ranking to Hoja 2
        actualizar_hoja(worksheet_ranking, 'A3', data_final)
        logging.info("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")

    except Exception as e:
        logging.error(f"Error en procesar_licitaciones_y_generar_ranking: {e}", exc_info=True)
        raise

# -------------------------- Elimination Function --------------------------

def eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas):
    """
    Removes licitaciones from Hoja 7 based on selected 'CodigoExterno' in Hoja 3.

    Args:
        worksheet_seleccion (gspread.Worksheet): Worksheet containing selected 'CodigoExterno'.
        worksheet_licitaciones_activas (gspread.Worksheet): Worksheet containing active licitaciones.
    """
    try:
        # Obtener 'CodigoExterno' seleccionados desde Hoja 3 (columna 1, desde fila 4)
        codigos_seleccionados = worksheet_seleccion.col_values(1)[3:]
        codigos_seleccionados_normalizados = set([
            eliminar_tildes_y_normalizar(codigo.lower()) for codigo in codigos_seleccionados if codigo
        ])
        logging.info(f"Total de 'CodigoExterno' seleccionados para eliminar: {len(codigos_seleccionados_normalizados)}")

        if not codigos_seleccionados_normalizados:
            logging.info("No hay 'CodigoExterno' seleccionados para eliminar.")
            return

        # Obtener todas las licitaciones activas de Hoja 7
        licitaciones = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones or len(licitaciones) < 2:
            logging.warning("No hay licitaciones en la Hoja 7 para procesar.")
            return

        # Convertir a DataFrame
        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Total de licitaciones en la Hoja 7 antes de filtrar: {len(df_licitaciones)}")

        if 'CodigoExterno' not in df_licitaciones.columns:
            logging.error("La columna 'CodigoExterno' no está presente en la Hoja 7.")
            return

        # Normalizar 'CodigoExterno' en DataFrame
        df_licitaciones['CodigoExterno_normalizado'] = df_licitaciones['CodigoExterno'].astype(str).apply(lambda x: eliminar_tildes_y_normalizar(x.lower()))

        # Filtrar licitaciones que están en 'codigos_seleccionados_normalizados'
        df_eliminadas = df_licitaciones[df_licitaciones['CodigoExterno_normalizado'].isin(codigos_seleccionados_normalizados)]
        num_eliminadas = len(df_eliminadas)
        logging.info(f"Total de licitaciones a eliminar de la Hoja 7: {num_eliminadas}")

        if num_eliminadas == 0:
            logging.info("No se encontraron licitaciones coincidentes para eliminar.")
            return

        # Filtrar las licitaciones que no están en 'codigos_seleccionados_normalizados'
        df_filtrado = df_licitaciones[~df_licitaciones['CodigoExterno_normalizado'].isin(codigos_seleccionados_normalizados)]
        logging.info(f"Total de licitaciones en la Hoja 7 después de filtrar: {len(df_filtrado)}")

        # Preparar datos para subir: incluir cabecera y eliminar la columna 'CodigoExterno_normalizado'
        data_to_upload = [df_filtrado.drop(columns=['CodigoExterno_normalizado']).columns.tolist()] + df_filtrado.drop(columns=['CodigoExterno_normalizado']).values.tolist()
        data_to_upload = [
            [serialize_value(x) for x in row]
            for row in data_to_upload
        ]

        # Limpiar y actualizar Hoja 7
        worksheet_licitaciones_activas.clear()
        logging.info("Contenido de la Hoja 7 borrado antes de actualizar con datos filtrados.")

        actualizar_hoja(worksheet_licitaciones_activas, 'A1', data_to_upload)
        logging.info(f"Se eliminaron {num_eliminadas} licitaciones seleccionadas de la Hoja 7.")
        print(f"Se eliminaron {num_eliminadas} licitaciones seleccionadas de la Hoja 7.")

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
        # Recuperar los valores de las celdas específicas
        ponderaciones_valores = obtener_rango_hoja(worksheet_inicio, 'K11:K43')
        # Asumiendo que las ponderaciones están en posiciones específicas dentro del rango
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
    try:
        # Cargar licitaciones desde Hoja 7
        licitaciones = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones or len(licitaciones) < 2:
            logging.warning("No hay licitaciones en la Hoja 7 para procesar.")
            return

        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Licitaciones cargadas desde la Hoja 7. Total: {len(df_licitaciones)}")

        # Filtrar 'TiempoDuracionContrato' != 0
        df_licitaciones = df_licitaciones[df_licitaciones['TiempoDuracionContrato'] != '0']
        logging.info(f"Filtradas licitaciones con 'TiempoDuracionContrato' != 0. Total: {len(df_licitaciones)}")

        # Excluir organizaciones de salud
        regex_excluir = re.compile('|'.join(SALUD_EXCLUIR), re.IGNORECASE)
        df_licitaciones = df_licitaciones[~df_licitaciones['NombreOrganismo'].str.contains(regex_excluir, na=False)]
        logging.info(f"Filtradas licitaciones relacionadas con salud. Total: {len(df_licitaciones)}")

        # Normalizar 'CodigoExterno' y otros campos relevantes
        for col in [ 'Nombre', 'Descripcion', 'Rubro3', 'Nombre producto genrico', 'NombreOrganismo']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = df_licitaciones[col].apply(lambda x: eliminar_tildes_y_normalizar(x) if isinstance(x, str) else x)

        logging.info("Campos relevantes, incluyendo 'CodigoExterno', normalizados.")

        # Convertir 'CodigoExterno' a string y manejar valores nulos
        df_licitaciones['CodigoExterno'] = df_licitaciones['CodigoExterno'].astype(str).str.strip()

        # Verificar si hay valores nulos en 'CodigoExterno'
        num_null_codes = df_licitaciones['CodigoExterno'].isnull().sum()
        if num_null_codes > 0:
            logging.warning(f"Hay {num_null_codes} valores nulos en 'CodigoExterno'.")
        else:
            logging.info("No hay valores nulos en 'CodigoExterno'.")

        # Agrupar por 'CodigoExterno' y sumarizar
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

        # Calcular puntajes
        df_licitaciones_agrupado['Puntaje Palabra'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_palabra(row, palabras_clave_set, lista_negra), axis=1
        )
        logging.info("Puntaje por palabras clave calculado.")

        df_licitaciones_agrupado['Puntaje Rubro'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_rubro(row, rubros_y_productos), axis=1
        )
        logging.info("Puntaje por rubros calculado.")

        df_licitaciones_agrupado['Puntaje Monto'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_monto(row['Tipo'], row['TiempoDuracionContrato']), axis=1
        )
        logging.info("Puntaje por monto calculado.")

        df_licitaciones_agrupado['Puntaje Clientes'] = df_licitaciones_agrupado['NombreOrganismo'].apply(
            lambda cliente: calcular_puntaje_clientes(cliente, puntaje_clientes)
        )
        logging.info("Puntaje por clientes calculado.")

        # Calcular puntaje total
        df_licitaciones_agrupado['Puntaje Total'] = (
            df_licitaciones_agrupado['Puntaje Rubro'] +
            df_licitaciones_agrupado['Puntaje Palabra'] +
            df_licitaciones_agrupado['Puntaje Monto'] +
            df_licitaciones_agrupado['Puntaje Clientes']
        )
        logging.info("Puntaje total calculado.")

        # Guardar puntajes NO relativos en Hoja 8
        df_no_relativos = df_licitaciones_agrupado[
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 
             'Puntaje Rubro', 'Puntaje Palabra', 
             'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ].copy()

        # Convertir a lista de listas y serializar
        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [
            [serialize_value(x) for x in row]
            for row in data_no_relativos
        ]

        # Limpiar Hoja 8 antes de subir
        worksheet_ranking_no_relativo.clear()
        logging.info("Hoja 8 (Ranking no relativo) limpiada exitosamente.")        

        # Subir los datos a Google Sheets
        actualizar_hoja(worksheet_ranking_no_relativo, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 8 exitosamente.")

        # -------------- Seleccionar Top 100 Licitaciones Únicas --------------

        # Ordenar licitaciones agrupadas por 'Puntaje Total' de manera descendente
        df_licitaciones_agrupado_sorted = df_licitaciones_agrupado.sort_values(
            by='Puntaje Total',
            ascending=False
        )
        logging.info("Licitaciones ordenadas por 'Puntaje Total' de manera descendente.")

        # Eliminar duplicados basados en 'CodigoExterno', manteniendo la primera ocurrencia (mayor puntaje)
        df_unique = df_licitaciones_agrupado_sorted.drop_duplicates(subset='CodigoExterno', keep='first')
        logging.info(f"Licitaciones después de eliminar duplicados: {len(df_unique)}")

        # Seleccionar las Top 100 licitaciones únicas
        df_top_100 = df_unique.head(100)
        logging.info("Top 100 licitaciones únicas seleccionadas.")

        # Verificar duplicados en df_top_100
        duplicate_codes_final = df_top_100[df_top_100.duplicated(subset='CodigoExterno', keep=False)]
        num_duplicates_final = len(duplicate_codes_final)
        if num_duplicates_final > 0:
            duplicate_codes_list = duplicate_codes_final['CodigoExterno'].unique().tolist()
            logging.warning(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
            print(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
        else:
            logging.info("No hay licitaciones duplicadas en el Top 100.")
            print("No hay licitaciones duplicadas en el Top 100.")

        # Calcular totales para cada criterio dentro del Top 100
        total_rubro = df_top_100['Puntaje Rubro'].sum()
        total_palabra = df_top_100['Puntaje Palabra'].sum()
        total_monto = df_top_100['Puntaje Monto'].sum()
        total_clientes = df_top_100['Puntaje Clientes'].sum()
        logging.info("Totales calculados para cada criterio dentro del Top 100.")

        # Ajustar puntajes relativos para que sumen 100
        df_top_100['Puntaje Relativo Rubro'] = df_top_100['Puntaje Rubro'].apply(
            lambda x: (x / total_rubro * 100) if total_rubro > 0 else 0
        )
        df_top_100['Puntaje Relativo Palabra'] = df_top_100['Puntaje Palabra'].apply(
            lambda x: (x / total_palabra * 100) if total_palabra > 0 else 0
        )
        df_top_100['Puntaje Relativo Monto'] = df_top_100['Puntaje Monto'].apply(
            lambda x: (x / total_monto * 100) if total_monto > 0 else 0
        )
        df_top_100['Puntaje Relativo Clientes'] = df_top_100['Puntaje Clientes'].apply(
            lambda x: (x / total_clientes * 100) if total_clientes > 0 else 0
        )
        logging.info("Puntajes relativos ajustados para que sumen 100.")

        # Calcular 'Puntaje Total SUMAPRODUCTO'
        df_top_100['Puntaje Total SUMAPRODUCTO'] = (
            df_top_100['Puntaje Relativo Rubro'] * ponderaciones.get('Puntaje Rubro', 0) +
            df_top_100['Puntaje Relativo Palabra'] * ponderaciones.get('Puntaje Palabra', 0) +
            df_top_100['Puntaje Relativo Monto'] * ponderaciones.get('Puntaje Monto', 0) +
            df_top_100['Puntaje Relativo Clientes'] * ponderaciones.get('Puntaje Clientes', 0)
        )
        logging.info("Puntaje Total SUMAPRODUCTO calculado.")

        # Ordenar Top 100 por 'Puntaje Total SUMAPRODUCTO'
        df_top_100 = df_top_100.sort_values(by='Puntaje Total SUMAPRODUCTO', ascending=False)
        logging.info("Top 100 licitaciones ordenadas por 'Puntaje Total SUMAPRODUCTO'.")

        # Rastrear la licitación en esta etapa
        rastrear_licitacion(df_top_100, CODIGO_A_RASTREAR, 'Después de Ordenar Top 100')

        # Crear estructura para Hoja 2
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

        # Verificar duplicados en df_final
        duplicate_codes_final = df_final[df_final.duplicated(subset='CodigoExterno', keep=False)]
        num_duplicates_final = len(duplicate_codes_final)
        if num_duplicates_final > 0:
            duplicate_codes_list = duplicate_codes_final['CodigoExterno'].unique().tolist()
            logging.warning(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
            print(f"Aún hay {num_duplicates_final} licitaciones duplicadas en el Top 100 con los siguientes CodigoExterno: {duplicate_codes_list}")
        else:
            logging.info("No hay licitaciones duplicadas en el Top 100.")
            print("No hay licitaciones duplicadas en el Top 100.")        

        # Asegurar formato correcto de decimales
        for col in ['Palabra', 'Monto', 'Puntaje Final']:
            df_final[col] = df_final[col].astype(float).round(2)

        # Convert to list of lists and serialize
        data_final = [df_final.columns.values.tolist()] + df_final.values.tolist()
        data_final = [
            [serialize_value(x) for x in row]
            for row in data_final
        ]

        # Preserve the value of A1 in Hoja 2
        nombre_a1 = worksheet_ranking.acell('A1').value if worksheet_ranking.acell('A1').value else ""

        # Clear Hoja 2 and restore A1
        worksheet_ranking.clear()
        worksheet_ranking.update('A1', [[nombre_a1]], value_input_option='USER_ENTERED')
        logging.info("Hoja 2 (Ranking) limpiada y A1 restaurado.")

        # Upload the final ranking to Hoja 2
        actualizar_hoja(worksheet_ranking, 'A3', data_final)
        logging.info("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")

    except Exception as e:
        logging.error(f"Error al generar el ranking: {e}", exc_info=True)
        raise

# -------------------------- Rastrear Licitacion Función --------------------------

def rastrear_licitacion(df, codigo_a_rastrear, etapa):
    """
    Rastrea una licitación específica y registra su estado en cada etapa.

    Args:
        df (pd.DataFrame): El DataFrame que representa las licitaciones actuales.
        codigo_a_rastrear (str): El CodigoExterno de la licitacion a rastrear.
        etapa (str): La etapa actual del procesamiento.
    """
    try:
        # Filtrar la licitación específica
        licitacion = df[df['CodigoExterno'] == codigo_a_rastrear]
        if not licitacion.empty:
            logging.info(f"Etapa: {etapa} - Licitación encontrada: {codigo_a_rastrear}")
            # Opcional: Registrar detalles adicionales si es necesario
            detalles = licitacion.to_dict('records')[0]
            logging.debug(f"Detalles de la licitación en etapa '{etapa}': {detalles}")
        else:
            logging.info(f"Etapa: {etapa} - Licitación no encontrada: {codigo_a_rastrear}")
    except KeyError:
        logging.error(f"La columna 'CodigoExterno' no existe en el DataFrame durante la etapa '{etapa}'.")
    except Exception as e:
        logging.error(f"Error al rastrear la licitación en la etapa '{etapa}': {e}", exc_info=True)

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
        # Recuperar los valores de las celdas específicas
        ponderaciones_valores = obtener_rango_hoja(worksheet_inicio, 'K11:K43')
        # Asumiendo que las ponderaciones están en posiciones específicas dentro del rango
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

# Nota: La función generar_ranking ya está incluida en el código anterior.
# Puedes eliminarla si no la utilizas, ya que toda la lógica está manejada en 'procesar_licitaciones_y_generar_ranking'.

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

        # Retrieve worksheets by name
        try:
            worksheet_inicio = get_worksheet_with_retry(sh, 'Inicio')                  # Hoja 1
            worksheet_ranking = get_worksheet_with_retry(sh, 'Ranking')                # Hoja 2
            worksheet_seleccion = get_worksheet_with_retry(sh, 'Selección')            # Hoja 3
            worksheet_rubros = get_worksheet_with_retry(sh, 'Rubros')                  # Hoja 4
            worksheet_clientes = get_worksheet_with_retry(sh, 'Clientes')              # Hoja 6
            worksheet_licitaciones_activas = get_worksheet_with_retry(sh, 'Licitaciones MP')  # Hoja 7
            worksheet_ranking_no_relativo = get_worksheet_with_retry(sh, 'Ranking no relativo')    # Hoja 8
            worksheet_lista_negra = get_worksheet_with_retry(sh, 'LNegra Palabras')        # Hoja 10
            worksheet_sicep = get_worksheet_with_retry(sh, 'Licitaciones Sicep')                     # Hoja 11
        except Exception as e:
            logging.error(f"Error al obtener una o más hojas: {e}", exc_info=True)
            raise

        # Execute the main processing
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
