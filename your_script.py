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

from extractores.sicep import login_and_scrape  # Asegúrate de que este módulo esté correctamente implementado

# -------------------------- Configuración de Constantes --------------------------

# Configuración de Logging
LOG_FILE = 'your_script.log'
LOG_MAX_BYTES = 10**6  # 1MB
LOG_BACKUP_COUNT = 5

# Configuración de Google Sheets
SHEET_ID = '1EGoDJtO-b5dAGzC8LRYyZVdhHdcE2_ukgZAl-Ni9IxM'
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_JSON"

# Configuración de URLs
BASE_URL = "https://transparenciachc.blob.core.windows.net/lic-da/"

# Organizaciones relacionadas con la salud para excluir
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

# Rango de Lista Negra (Blacklist)
LISTA_NEGRA_RANGE = 'B2'

# Rango de Fechas
FECHAS_RANGE = 'C6:C7'

# Rangos de Palabras Clave
PALABRAS_CLAVE_RANGES = {
    'C': 'C27:C36',
    'F': 'F27:F36',
    'I': 'I27:I36'
}

# Rangos de Rubros y Productos
RUBROS_RANGES = {
    'rubro1': 'C13',
    'rubro2': 'F13',
    'rubro3': 'I13'
}
PRODUCTOS_RANGES = {
    'rubro1': ['D14', 'D15', 'D16', 'D17', 'D18', 'D19', 'D20', 'D21', 'D22', 'D23'],
    'rubro2': ['G14', 'G15', 'G16', 'G17', 'G18', 'G19', 'G20', 'G21', 'G22', 'G23'],
    'rubro3': ['J14', 'J15', 'J16', 'J17', 'J18', 'J19', 'J20', 'J21', 'J22', 'J23']
}

# Columnas Importantes
COLUMNAS_IMPORTANTES = [
    'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCreacion', 'FechaCierre',
    'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico', 'CodigoProductoONU',
    'Tipo', 'CantidadReclamos', 'TiempoDuracionContrato', 'Link'
]

# -------------------------- Configuración de Logging --------------------------

def setup_logging():
    """
    Configura el logging con rotación para evitar que el archivo de log crezca indefinidamente.
    """
    handler = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging está configurado e inicializado.")

# -------------------------- Autenticación con Google Sheets --------------------------

def authenticate_google_sheets():
    """
    Autentica con Google Sheets usando credenciales de cuenta de servicio.

    Returns:
        gspread.Client: Un cliente autorizado de gspread.
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

# -------------------------- Recuperación de Worksheets con Retry --------------------------

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def obtener_worksheet(spreadsheet, nombre):
    """
    Recupera una hoja por nombre con manejo de errores.

    Args:
        spreadsheet (gspread.Spreadsheet): El objeto de la hoja de cálculo.
        nombre (str): El nombre de la hoja a recuperar.

    Returns:
        gspread.Worksheet: La hoja recuperada.
    """
    try:
        worksheet = spreadsheet.worksheet(nombre)
        logging.info(f"Hoja '{nombre}' obtenida exitosamente.")
        return worksheet
    except WorksheetNotFound:
        logging.error(f"Hoja '{nombre}' no encontrada.")
        raise
    except APIError as e:
        logging.warning(f"APIError al obtener la hoja '{nombre}': {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al obtener la hoja '{nombre}': {e}", exc_info=True)
        raise

# -------------------------- Funciones Utilitarias --------------------------

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
        texto = re.sub(r'\b(\w+)\b(?=.*\b\1\b)', '', texto)  # Eliminar palabras repetidas
        texto = texto.strip().lower()
    return texto

def obtener_rango_hoja(worksheet, rango):
    """
    Recupera valores de un rango especificado en una hoja. Soporta rangos simples.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo.
        rango (str): El rango en notación A1.

    Returns:
        list: Lista de listas con los valores obtenidos.
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
    Recupera y procesa frases clave desde rangos especificados en la hoja.

    Args:
        worksheet_inicio (gspread.Worksheet): La hoja de cálculo de la cual recuperar las palabras clave.

    Returns:
        set: Un conjunto de frases clave procesadas.
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
    Recupera las frases de la lista negra desde el rango especificado en la hoja.

    Args:
        worksheet_lista_negra (gspread.Worksheet): La hoja de cálculo de la cual recuperar la lista negra.

    Returns:
        set: Un conjunto de frases de la lista negra.
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
    Recupera rubros y sus correspondientes productos desde la hoja de forma eficiente utilizando batch_get.

    Args:
        worksheet_rubros (gspread.Worksheet): La hoja de cálculo de la cual recuperar rubros y productos.

    Returns:
        dict: Un diccionario mapeando rubros a sus listas de productos.
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
    Recupera clientes y sus estados desde la hoja y asigna puntajes.

    Args:
        worksheet_clientes (gspread.Worksheet): Hoja de cálculo que contiene datos de clientes.

    Returns:
        dict: Diccionario mapeando clientes a sus puntajes basados en el estado.
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

def obtener_ponderaciones(worksheet_inicio):
    """
    Recupera ponderaciones desde la Hoja 1.

    Args:
        worksheet_inicio (gspread.Worksheet): La hoja de cálculo que contiene las ponderaciones.

    Returns:
        dict: Un diccionario con las ponderaciones.
    """
    try:
        # Recuperar los valores de las celdas específicas
        ponderaciones_valores = {
            'Puntaje Rubro': obtener_rango_hoja(worksheet_inicio, 'K11')[0][0],
            'Puntaje Palabra': obtener_rango_hoja(worksheet_inicio, 'K25')[0][0],
            'Puntaje Clientes': obtener_rango_hoja(worksheet_inicio, 'K39')[0][0],
            'Puntaje Monto': obtener_rango_hoja(worksheet_inicio, 'K43')[0][0]
        }

        # Convertir los valores a float después de eliminar el '%'
        ponderaciones = {
            key: float(value.strip('%')) / 100 for key, value in ponderaciones_valores.items() if value
        }

        logging.info(f"Ponderaciones obtenidas: {ponderaciones}")
        return ponderaciones
    except Exception as e:
        logging.error(f"Error al obtener ponderaciones: {e}", exc_info=True)
        raise

def obtener_rango_disjunto(worksheet, rangos):
    """
    Obtiene valores de celdas disjuntas de una hoja de cálculo utilizando batch_get.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo.
        rangos (list): Lista de rangos a recuperar.

    Returns:
        list: Lista de valores obtenidos para cada rango.
    """
    try:
        valores = worksheet.batch_get(rangos)
        for rango in rangos:
            logging.info(f"Valores obtenidos del rango {rango}.")
        return [val for sublist in valores for val in sublist]  # Aplanar la lista
    except Exception as e:
        logging.error(f"Error al obtener valores de rangos disjuntos {rangos}: {e}", exc_info=True)
        raise

# -------------------------- Funciones de Calculo de Puntajes --------------------------

def calcular_puntaje_palabra(row, palabras_clave, lista_negra):
    """
    Calcula el puntaje basado en la presencia de palabras clave en el nombre y la descripción de la licitación,
    excluyendo palabras de la lista negra.

    Args:
        row (pd.Series): Una fila del DataFrame representando una licitación.
        palabras_clave (set): Conjunto de palabras clave a buscar.
        lista_negra (set): Conjunto de palabras para excluir.

    Returns:
        int: El puntaje calculado basado en palabras clave.
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
        palabras_encontradas = palabras_clave.intersection(palabras_texto)
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
    Calcula el puntaje basado en rubros y productos.
    Asigna +5 si se encuentra el rubro.
    Asigna +10 adicional si se encuentra el producto asociado al rubro.

    Args:
        row (pd.Series): Una fila del DataFrame representando una licitación.
        rubros_y_productos (dict): Diccionario mapeando rubros a productos.

    Returns:
        int: El puntaje calculado.
    """
    try:
        rubro_column = eliminar_tildes_y_normalizar(row['Rubro3']) if pd.notnull(row['Rubro3']) else ''
        productos_column = eliminar_tildes_y_normalizar(row['CodigoProductoONU']) if pd.notnull(row['CodigoProductoONU']) else ''
        puntaje_rubro = 0

        logging.debug(f"Evaluando fila: Rubro='{rubro_column}', CodigoProducto='{productos_column}'")

        rubros_presentes = set()
        productos_presentes = set()

        for rubro, productos in rubros_y_productos.items():
            if rubro in rubro_column:
                rubros_presentes.add(rubro)
                logging.info(f"Rubro encontrado: {rubro} en '{rubro_column}'")

                for producto in productos:
                    if producto in productos_column:
                        productos_presentes.add(producto)
                        logging.info(f"Producto encontrado: '{producto}' asociado a rubro '{rubro}'")

        if not rubros_presentes and not productos_presentes:
            logging.warning(f"No se encontraron coincidencias para Rubro3='{rubro_column}' ni CodigoProductoONU='{productos_column}'.")

        puntaje_rubro += len(rubros_presentes) * 5
        puntaje_rubro += len(productos_presentes) * 10

        logging.debug(f"Puntaje calculado para rubros: {puntaje_rubro}")
        return puntaje_rubro
    except Exception as e:
        logging.error(f"Error al calcular puntaje por rubro: {e}", exc_info=True)
        return 0

def calcular_puntaje_monto(tipo_licitacion, tiempo_duracion_contrato):
    """
    Calcula el puntaje basado en el monto de la licitación.

    Args:
        tipo_licitacion (str): Tipo de licitación.
        tiempo_duracion_contrato (str): Duración del contrato.

    Returns:
        float: El puntaje calculado.
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
    Recupera el puntaje del cliente basado en el nombre del organismo.

    Args:
        nombre_organismo (str): Nombre del organismo.
        puntaje_clientes (dict): Diccionario mapeando clientes a puntajes.

    Returns:
        int: El puntaje asignado al cliente.
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

# -------------------------- Actualización de Google Sheets con Retry --------------------------

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
        worksheet.update(rango, datos)
        logging.info(f"Hoja actualizada exitosamente en el rango {rango}.")
    except APIError as e:
        logging.warning(f"APIError al actualizar la Hoja en el rango {rango}: {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error al actualizar la Hoja en el rango {rango}: {e}", exc_info=True)
        raise

# -------------------------- Funciones de Recuperación y Procesamiento de Datos --------------------------

def procesar_licitaciones(url):
    """
    Descarga y procesa un archivo ZIP que contiene CSVs de licitaciones.

    Args:
        url (str): La URL para descargar el archivo ZIP.

    Returns:
        pd.DataFrame: Un DataFrame concatenado de todos los CSVs procesados.
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
    Integra licitaciones de SICEP en la hoja designada.

    Args:
        worksheet_sicep (gspread.Worksheet): La hoja donde se subirán las licitaciones de SICEP.

    Returns:
        pd.DataFrame: El DataFrame de licitaciones de SICEP.
    """
    try:
        df_sicep = login_and_scrape().rename(columns={
            "Titulo": "Nombre",
            "Fecha de Publicacion": "FechaCreacion",
            "Fecha de Cierre": "FechaCierre",
            "Descripcion": "Descripcion",
            "Link": "Link"
        })

        # Asegurar columnas obligatorias
        columnas_obligatorias = [
            "CodigoExterno", "Nombre", "Descripcion", "CodigoEstado",
            "NombreOrganismo", "Tipo", "CantidadReclamos", "FechaCreacion",
            "FechaCierre", "TiempoDuracionContrato", "Rubro3", "Nombre producto genrico", "Link"
        ]
        for columna in columnas_obligatorias:
            if columna not in df_sicep.columns:
                df_sicep[columna] = None

        # Convertir a lista de listas para Google Sheets
        data_to_upload = [df_sicep.columns.values.tolist()] + df_sicep.values.tolist()
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]

        # Limpiar y actualizar la hoja
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

# -------------------------- Funciones de Gestión de Licitaciones --------------------------

def eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas):
    """
    Elimina licitaciones de la Hoja 7 basándose en los 'CodigoExterno' seleccionados en la Hoja 3.

    Args:
        worksheet_seleccion (gspread.Worksheet): Hoja 3 que contiene los 'CodigoExterno' seleccionados.
        worksheet_licitaciones_activas (gspread.Worksheet): Hoja 7 que contiene las licitaciones activas.
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
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]

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


# -------------------------- Función Principal de Procesamiento --------------------------

def procesar_licitaciones_y_generar_ranking(
    worksheet_inicio,
    worksheet_ranking,
    worksheet_seleccion,
    worksheet_rubros,
    worksheet_clientes,
    worksheet_licitaciones_activas,
    worksheet_ranking_no_relativo,
    worksheet_lista_negra,
    worksheet_sicep
):
    """
    Procesa los datos de licitaciones y genera un ranking basado en varios criterios.

    Args:
        worksheet_inicio (gspread.Worksheet): Hoja 1 con configuraciones iniciales.
        worksheet_ranking (gspread.Worksheet): Hoja 2 para subir el ranking final.
        worksheet_seleccion (gspread.Worksheet): Hoja 3 con licitaciones seleccionadas.
        worksheet_rubros (gspread.Worksheet): Hoja 4 con datos de rubros.
        worksheet_clientes (gspread.Worksheet): Hoja 6 con datos de clientes.
        worksheet_licitaciones_activas (gspread.Worksheet): Hoja 7 con licitaciones activas.
        worksheet_ranking_no_relativo (gspread.Worksheet): Hoja 8 para subir puntajes no relativos.
        worksheet_lista_negra (gspread.Worksheet): Hoja 10 con palabras de la lista negra.
        worksheet_sicep (gspread.Worksheet): Hoja 11 con licitaciones de Sicep.
    """
    try:
        # Extraer fechas mínimas de la Hoja 1
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

        # -------------- Eliminar Licitaciones Seleccionadas --------------
        # Llamar a la función para eliminar licitaciones seleccionadas antes de procesar
        eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas)

        # Volver a obtener las licitaciones activas después de la eliminación
        licitaciones_actualizadas = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones_actualizadas or len(licitaciones_actualizadas) < 2:
            logging.warning("No hay licitaciones activas después de la eliminación.")
            return

        df_licitaciones = pd.DataFrame(licitaciones_actualizadas[1:], columns=licitaciones_actualizadas[0])
        logging.info(f"Total de licitaciones activas después de eliminar seleccionadas: {len(df_licitaciones)}")

        # Normalizar y limpiar las columnas antes de procesar
        for col in ['Nombre', 'Descripcion', 'Rubro3', 'Nombre producto genrico', 'NombreOrganismo']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = df_licitaciones[col].apply(lambda x: eliminar_tildes_y_normalizar(x) if isinstance(x, str) else x)
        
        if 'CodigoProductoONU' in df_licitaciones.columns:
            df_licitaciones['CodigoProductoONU'] = df_licitaciones['CodigoProductoONU'].apply(lambda x: eliminar_tildes_y_normalizar(str(x).split('.')[0]) if pd.notnull(x) else x)

        # Convertir las columnas de fechas a tipo datetime
        for col in ['FechaCreacion', 'FechaCierre']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = pd.to_datetime(df_licitaciones[col], errors='coerce')

        # Filtrar por fechas de publicación y cierre
        df_licitaciones = df_licitaciones[
            (df_licitaciones['FechaCreacion'] >= fecha_min_publicacion) &
            (df_licitaciones['FechaCierre'] >= fecha_min_cierre)
        ]
        logging.info(f"Total de licitaciones después de aplicar filtros de fecha: {len(df_licitaciones)}")

        # -------------- Filtrar Licitaciones de Organizaciones de Salud (Vectorizado) --------------

        # Crear un patrón regex para los términos de exclusión
        patron_exclusion = '|'.join([re.escape(termino.lower()) for termino in SALUD_EXCLUIR])

        # Normalizar 'NombreOrganismo' para el filtro
        df_licitaciones['NombreOrganismo_normalizado'] = df_licitaciones['NombreOrganismo'].astype(str).apply(lambda x: eliminar_tildes_y_normalizar(x))

        # Aplicar el filtro utilizando str.contains con case insensitivity
        df_filtrado_salud = df_licitaciones[
            df_licitaciones['NombreOrganismo_normalizado'].str.contains(patron_exclusion, case=False, na=False)
        ]
        num_filtradas_salud = len(df_filtrado_salud)
        df_licitaciones = df_licitaciones[
            ~df_licitaciones['NombreOrganismo_normalizado'].str.contains(patron_exclusion, case=False, na=False)
        ]
        logging.info(f"Total de licitaciones después de excluir organizaciones de salud: {len(df_licitaciones)}")
        logging.info(f"Total de licitaciones filtradas por salud: {num_filtradas_salud}")

        # Continuar con el cálculo de puntajes y generación del ranking...

        # Calcular puntajes
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

        # Guardar puntajes NO relativos en Hoja 8
        df_no_relativos = df_licitaciones[
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 
             'Puntaje Rubro', 'Puntaje Palabra', 
             'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ]

        # Convertir a números y limpiar formatos incorrectos
        df_no_relativos = df_no_relativos.applymap(
            lambda x: float(x) if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()) else x
        )
        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [[str(x).replace("'", "") for x in row] for row in data_no_relativos]

        actualizar_hoja(worksheet_ranking_no_relativo, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 8 exitosamente.")

        # Seleccionar Top 100 licitaciones
        df_top_100 = df_licitaciones.sort_values(
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
        df_top_100['Puntaje Relativo Rubro'] = (df_top_100['Puntaje Rubro'] / total_rubro * 100) if total_rubro > 0 else 0
        df_top_100['Puntaje Relativo Palabra'] = (df_top_100['Puntaje Palabra'] / total_palabra * 100) if total_palabra > 0 else 0
        df_top_100['Puntaje Relativo Monto'] = (df_top_100['Puntaje Monto'] / total_monto * 100) if total_monto > 0 else 0
        df_top_100['Puntaje Relativo Clientes'] = (df_top_100['Puntaje Clientes'] / total_clientes * 100) if total_clientes > 0 else 0
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

        # Asegurar formato correcto de decimales
        for col in ['Palabra', 'Monto', 'Puntaje Final']:
            df_final[col] = df_final[col].astype(float).round(2)

        data_final = [df_final.columns.values.tolist()] + df_final.values.tolist()
        data_final = [[str(x).replace("'", "") if isinstance(x, str) else x for x in row] for row in data_final]

        # Preservar el valor de la celda A1 en Hoja 2
        nombre_a1 = worksheet_ranking.acell('A1').value if worksheet_ranking.acell('A1').value else ""

        # Limpiar Hoja 2 y restaurar A1
        worksheet_ranking.clear()
        worksheet_ranking.update('A1', [[nombre_a1]])
        logging.info("Hoja 2 limpiada y A1 restaurado.")

        # Subir el ranking final a Hoja 2
        actualizar_hoja(worksheet_ranking, 'A3', data_final)
        logging.info("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")

    except Exception as e:
        logging.error(f"Error en procesar_licitaciones_y_generar_ranking: {e}", exc_info=True)
        raise


# -------------------------- Función Principal --------------------------

def main():
    """
    Función principal que orquesta todo el flujo de procesamiento.
    """
    try:
        # Configurar logging
        setup_logging()

        # Autenticar con Google Sheets
        gc = authenticate_google_sheets()

        # Abrir la hoja de cálculo
        try:
            sh = gc.open_by_key(SHEET_ID)
            logging.info(f"Spreadsheet con ID {SHEET_ID} abierto exitosamente.")
        except SpreadsheetNotFound as e:
            logging.error(f"Spreadsheet con ID {SHEET_ID} no encontrado: {e}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Error al abrir Spreadsheet: {e}", exc_info=True)
            raise

        # Recuperar todas las worksheets necesarias por nombre
        try:
            worksheet_inicio = obtener_worksheet(sh, 'Inicio')                  # Hoja 1
            worksheet_ranking = obtener_worksheet(sh, 'Ranking')                # Hoja 2
            worksheet_seleccion = obtener_worksheet(sh, 'Selección')            # Hoja 3
            worksheet_rubros = obtener_worksheet(sh, 'Rubros')                  # Hoja 4
            worksheet_clientes = obtener_worksheet(sh, 'Clientes')              # Hoja 6
            worksheet_licitaciones_activas = obtener_worksheet(sh, 'Licitaciones MP')  # Hoja 7
            worksheet_ranking_no_relativo = obtener_worksheet(sh, 'Ranking no relativo')    # Hoja 8
            worksheet_lista_negra = obtener_worksheet(sh, 'LNegra Palabras')        # Hoja 10
            worksheet_sicep = obtener_worksheet(sh, 'Licitaciones Sicep')                     # Hoja 11
        except Exception as e:
            logging.error(f"Error al obtener una o más hojas: {e}", exc_info=True)
            raise

        # Ejecutar el procesamiento principal
        procesar_licitaciones_y_generar_ranking(
            worksheet_inicio,
            worksheet_ranking,
            worksheet_seleccion,
            worksheet_rubros,
            worksheet_clientes,
            worksheet_licitaciones_activas,
            worksheet_ranking_no_relativo,
            worksheet_lista_negra,
            worksheet_sicep
        )

    except Exception as e:
        logging.critical(f"Script finalizado con errores: {e}", exc_info=True)
        raise

# -------------------------- Punto de Entrada --------------------------

if __name__ == "__main__":
    main()
