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
LISTA_NEGRA_RANGE = 'B2:B'

# Rango de Fechas
FECHAS_RANGE = 'C6:C7'

# Rangos de Palabras Clave
PALABRAS_CLAVE_RANGES = {
    'C': 'C27:C32',
    'F': 'F27:F35',
    'I': 'I27:I34'
}

# Rangos de Rubros y Productos
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

# Columnas Importantes
COLUMNAS_IMPORTANTES = [
    'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCreacion', 'FechaCierre',
    'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico',
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
def get_worksheet_with_retry(spreadsheet, index):
    """
    Recupera una hoja por índice con un mecanismo de retry para manejar errores de la API.

    Args:
        spreadsheet (gspread.Spreadsheet): El objeto de la hoja de cálculo.
        index (int): El índice de la hoja a recuperar.

    Returns:
        gspread.Worksheet: La hoja recuperada.
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

# -------------------------- Funciones Utilitarias --------------------------

def eliminar_tildes(texto):
    """
    Elimina las tildes de una cadena de texto.

    Args:
        texto (str): El texto a procesar.

    Returns:
        str: El texto sin tildes.
    """
    if texto:
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(char for char in texto if unicodedata.category(char) != 'Mn')
    return texto

def obtener_rango_hoja(worksheet, rango):
    """
    Recupera valores de un rango especificado en una hoja. Soporta rangos simples.
    """
    try:
        if ',' in rango:  # Si el rango es disjunto (contiene ',')
            rangos_individuales = rango.split(',')
            valores = []
            for rango_individual in rangos_individuales:
                valores.extend(worksheet.get(rango_individual))
            logging.info(f"Valores obtenidos de los rangos disjuntos: {rango}")
            return valores
        else:
            valores = worksheet.get(rango)
            logging.info(f"Valores obtenidos del rango {rango}.")
            return valores
    except Exception as e:
        logging.error(f"Error al obtener valores del rango {rango}: {e}", exc_info=True)
        raise

def obtener_palabras_clave(worksheet):
    """
    Recupera y procesa frases clave desde rangos especificados en la hoja.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo de la cual recuperar las palabras clave.

    Returns:
        set: Un conjunto de frases clave procesadas.
    """
    try:
        palabras_clave = []
        for key, rango in PALABRAS_CLAVE_RANGES.items():
            valores = obtener_rango_hoja(worksheet, rango)
            palabras_clave.extend([p.lower() for fila in valores for p in fila if p])
        palabras_clave_set = set(palabras_clave)
        logging.info(f"Palabras clave obtenidas: {palabras_clave_set}")
        return palabras_clave_set
    except Exception as e:
        logging.error(f"Error al obtener palabras clave: {e}", exc_info=True)
        raise

def obtener_lista_negra(worksheet):
    """
    Recupera las frases de la lista negra desde el rango especificado en la hoja.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo de la cual recuperar la lista negra.

    Returns:
        set: Un conjunto de frases de la lista negra.
    """
    try:
        data_lista_negra = obtener_rango_hoja(worksheet, LISTA_NEGRA_RANGE)
        lista_negra = set([row[0].strip().lower() for row in data_lista_negra if row and row[0].strip()])
        logging.info(f"Lista negra obtenida: {lista_negra}")
        return lista_negra
    except Exception as e:
        logging.error(f"Error al obtener la lista negra: {e}", exc_info=True)
        raise

def obtener_rubros_y_productos(worksheet):
    """
    Recupera rubros y sus correspondientes productos desde la hoja de forma eficiente.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo de la cual recuperar rubros y productos.

    Returns:
        dict: Un diccionario mapeando rubros a sus listas de productos.
    """
    try:
        # Obtener rangos en un solo batch
        rangos = list(RUBROS_RANGES.values()) + [','.join(PRODUCTOS_RANGES[key]) for key in PRODUCTOS_RANGES]
        valores_rangos = worksheet.batch_get(rangos)

        # Extraer rubros
        rubros = {
            key: valores[0][0].strip() if valores and valores[0][0] else None
            for key, valores in zip(RUBROS_RANGES.keys(), valores_rangos[:len(RUBROS_RANGES)])
        }

        # Extraer productos
        productos = {
            key: [item[0].strip().lower() for item in valores if item and item[0].strip()]
            for key, valores in zip(PRODUCTOS_RANGES.keys(), valores_rangos[len(RUBROS_RANGES):])
        }

        # Mapear rubros a productos
        rubros_y_productos = {
            eliminar_tildes(rubro.lower()): productos.get(key, [])
            for key, rubro in rubros.items()
            if rubro
        }

        logging.info(f"Rubros y productos obtenidos: {rubros_y_productos}")
        return rubros_y_productos

    except Exception as e:
        logging.error(f"Error al obtener rubros y productos: {e}", exc_info=True)
        raise

def obtener_puntaje_clientes(worksheet):
    """
    Recupera clientes y sus estados desde la hoja y asigna puntajes.

    Args:
        worksheet (gspread.Worksheet): La hoja de cálculo de la cual recuperar datos de clientes.

    Returns:
        dict: Un diccionario mapeando clientes a sus puntajes basados en el estado.
    """
    try:
        # Recuperar todas las filas de clientes y estados de una vez
        clientes = worksheet.col_values(4)[3:]  # D4 en adelante
        estados = worksheet.col_values(5)[3:]   # E4 en adelante

        puntaje_clientes = {}
        for cliente, estado in zip(clientes, estados):
            estado_lower = estado.strip().lower()
            if estado_lower == 'vigente':
                puntaje_clientes[eliminar_tildes(cliente.lower())] = 10
            elif estado_lower == 'no vigente':
                puntaje_clientes[eliminar_tildes(cliente.lower())] = 5
            else:
                puntaje_clientes[eliminar_tildes(cliente.lower())] = 0
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

# -------------------------- Funciones de Calculo de Puntajes --------------------------

def calcular_puntaje_palabra(nombre, descripcion, palabras_clave_set, lista_negra):
    """
    Calcula el puntaje basado en palabras clave y aplica penalizaciones según la lista negra.

    Args:
        nombre (str): Nombre de la licitación.
        descripcion (str): Descripción de la licitación.
        palabras_clave_set (set): Conjunto de palabras clave.
        lista_negra (set): Conjunto de frases de la lista negra.

    Returns:
        int: El puntaje calculado.
    """
    try:
        texto = f"{nombre.lower()} {descripcion.lower()}"
        
        # Aplicar penalización específica
        if "consumo humano" in texto:
            logging.info(f"Penalización aplicada: frase 'consumo humano' encontrada en '{texto}'")
            return -10
        
        # Sumar puntos por palabras clave
        palabras_texto = set(re.findall(r'\b\w+\b', texto))
        palabras_encontradas = palabras_clave_set.intersection(palabras_texto)
        puntaje_palabra = len(palabras_encontradas) * 10
        
        for palabra in palabras_encontradas:
            logging.info(f"Puntos sumados por palabra clave: '{palabra}' en '{texto}'")
        
        return puntaje_palabra
    except Exception as e:
        logging.error(f"Error al calcular puntaje por palabra: {e}", exc_info=True)
        return 0

def calcular_puntaje_rubro(row, rubros_y_productos):
    """
    Calcula el puntaje basado en rubros y productos.

    Args:
        row (pd.Series): Una fila del DataFrame representando una licitación.
        rubros_y_productos (dict): Diccionario mapeando rubros a productos.

    Returns:
        int: El puntaje calculado.
    """
    try:
        rubro_column = eliminar_tildes(row['Rubro3'].lower()) if pd.notnull(row['Rubro3']) else ''
        productos_column = eliminar_tildes(row['Nombre producto genrico'].lower()) if pd.notnull(row['Nombre producto genrico']) else ''
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
    Recupera el puntaje del cliente basado en el nombre del organismo.

    Args:
        nombre_organismo (str): Nombre del organismo.
        puntaje_clientes (dict): Diccionario mapeando clientes a puntajes.

    Returns:
        int: El puntaje asignado al cliente.
    """
    try:
        return puntaje_clientes.get(eliminar_tildes(nombre_organismo.lower()), 0)
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
        worksheet.update(range_name=rango, values=datos)
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
            "Link", "CodigoExterno", "Nombre", "Descripcion", "CodigoEstado",
            "NombreOrganismo", "Tipo", "CantidadReclamos", "FechaCreacion",
            "FechaCierre", "TiempoDuracionContrato", "Rubro3", "Nombre producto genrico"
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

def eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas):
    """
    Elimina licitaciones de la Hoja 7 basándose en los 'CodigoExterno' seleccionados en la Hoja 6.

    Args:
        worksheet_seleccion (gspread.Worksheet): Hoja 6 que contiene los 'CodigoExterno' seleccionados.
        worksheet_licitaciones_activas (gspread.Worksheet): Hoja 7 que contiene las licitaciones activas.
    """
    try:
        codigos_seleccionados = worksheet_seleccion.col_values(1)[3:]  # Asumiendo que los 'CodigoExterno' están en la primera columna a partir de la fila 4
        codigos_seleccionados = set([eliminar_tildes(codigo.lower()) for codigo in codigos_seleccionados if codigo])
        logging.info(f"Total de 'CodigoExterno' seleccionados para eliminar: {len(codigos_seleccionados)}")

        licitaciones = worksheet_licitaciones_activas.get_all_values()
        if not licitaciones or len(licitaciones) < 2:
            logging.warning("No hay licitaciones en la Hoja 7 para procesar.")
            return

        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Total de licitaciones en la Hoja 7 antes de filtrar: {len(df_licitaciones)}")

        df_licitaciones_filtrado = df_licitaciones[~df_licitaciones['CodigoExterno'].str.lower().isin(codigos_seleccionados)]
        logging.info(f"Total de licitaciones en la Hoja 7 después de filtrar: {len(df_licitaciones_filtrado)}")

        # Preparar datos para subir
        data_to_upload = [df_licitaciones_filtrado.columns.values.tolist()] + df_licitaciones_filtrado.values.tolist()
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]

        # Actualizar la Hoja 7
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

# -------------------------- Función Principal de Procesamiento --------------------------

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
    Procesa los datos de licitaciones y genera un ranking basado en varios criterios.

    Args:
        worksheet_inicio (gspread.Worksheet): Hoja 1 con configuraciones iniciales.
        worksheet_ranking (gspread.Worksheet): Hoja 2 para subir el ranking final.
        worksheet_rubros (gspread.Worksheet): Hoja 3 con datos de rubros.
        worksheet_clientes (gspread.Worksheet): Hoja 4 con datos de clientes.
        worksheet_seleccion (gspread.Worksheet): Hoja 6 con licitaciones seleccionadas.
        worksheet_licitaciones_activas (gspread.Worksheet): Hoja 7 con licitaciones activas y duplicadas.
        worksheet_ranking_no_relativo (gspread.Worksheet): Hoja 8 para subir puntajes no relativos.
        worksheet_lista_negra (gspread.Worksheet): Hoja 10 con palabras de la lista negra.
        worksheet_sicep (gspread.Worksheet): Hoja 11 con licitaciones de SICEP.
    """
    try:
        # Extraer fechas mínimas de la Hoja 1
        valores_fechas = obtener_rango_hoja(worksheet_inicio, FECHAS_RANGE)
        fecha_min_publicacion = pd.to_datetime(valores_fechas[0][0], errors='coerce')
        fecha_min_cierre = pd.to_datetime(valores_fechas[1][0], errors='coerce')
        logging.info(f"Fecha mínima de publicación: {fecha_min_publicacion}")
        logging.info(f"Fecha mínima de cierre: {fecha_min_cierre}")

        # Determinar mes y año actual y anterior
        now = datetime.now()
        mes_actual = now.month
        año_actual = now.year

        if mes_actual == 1:
            mes_anterior = 12
            año_anterior = año_actual - 1
        else:
            mes_anterior = mes_actual - 1
            año_anterior = año_actual

        # Construir URLs
        url_mes_actual = f"{BASE_URL}{año_actual}-{mes_actual:02d}.zip"
        url_mes_anterior = f"{BASE_URL}{año_anterior}-{mes_anterior:02d}.zip"

        logging.info(f"URL del mes actual: {url_mes_actual}")
        logging.info(f"URL del mes anterior: {url_mes_anterior}")

        # Descargar y procesar licitaciones
        df_mes_actual = procesar_licitaciones(url_mes_actual)
        df_mes_anterior = procesar_licitaciones(url_mes_anterior)

        # Integrar licitaciones de SICEP
        df_sicep = integrar_licitaciones_sicep(worksheet_sicep)

        # Concatenar todas las licitaciones
        df_licitaciones = pd.concat([df_mes_actual, df_mes_anterior, df_sicep], ignore_index=True)
        logging.info(f"Total de licitaciones después de concatenar: {len(df_licitaciones)}")

        # Eliminar tildes y convertir a minúsculas
        df_licitaciones['Nombre'] = df_licitaciones['Nombre'].apply(lambda x: eliminar_tildes(x.lower()) if isinstance(x, str) else x)
        df_licitaciones['Descripcion'] = df_licitaciones['Descripcion'].apply(lambda x: eliminar_tildes(x.lower()) if isinstance(x, str) else x)

        # Filtrar licitaciones con 'CodigoEstado' = 5
        if 'CodigoEstado' in df_licitaciones.columns:
            df_licitaciones = df_licitaciones[df_licitaciones['CodigoEstado'] == 5]
            logging.info(f"Filtradas licitaciones con 'CodigoEstado' = 5. Total: {len(df_licitaciones)}")
        else:
            df_licitaciones = pd.DataFrame()
            logging.warning("'CodigoEstado' no está en las columnas. Se creó un DataFrame vacío.")

        # Seleccionar columnas importantes
        df_licitaciones = df_licitaciones[df_licitaciones.columns.intersection(COLUMNAS_IMPORTANTES)]

        # Añadir columnas faltantes con None
        for columna in COLUMNAS_IMPORTANTES:
            if columna not in df_licitaciones.columns:
                df_licitaciones[columna] = None

        logging.info(f"Seleccionadas columnas importantes. Total de licitaciones: {len(df_licitaciones)}")

        # Convertir columnas de fechas
        for col in ['FechaCreacion', 'FechaCierre']:
            if col in df_licitaciones.columns:
                df_licitaciones[col] = pd.to_datetime(df_licitaciones[col], errors='coerce')

        # Eliminar filas con fechas inválidas
        df_licitaciones.dropna(subset=['FechaCreacion', 'FechaCierre'], inplace=True)
        logging.info("Fechas convertidas y filas con fechas inválidas eliminadas.")

        # Aplicar filtros mínimos de fechas
        df_licitaciones = df_licitaciones[df_licitaciones['FechaCreacion'] >= fecha_min_publicacion]
        logging.info(f"Fechas después del filtro de publicación: {df_licitaciones['FechaCreacion'].unique()}")

        df_licitaciones = df_licitaciones[df_licitaciones['FechaCierre'] >= fecha_min_cierre]
        logging.info(f"Licitaciones filtradas: {len(df_licitaciones)} después de aplicar los filtros de publicación y cierre.")

        # Subir licitaciones filtradas a Hoja 7
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

        # Eliminar licitaciones seleccionadas de Hoja 7
        eliminar_licitaciones_seleccionadas(worksheet_seleccion, worksheet_licitaciones_activas)

        # Obtener datos necesarios para el puntaje
        palabras_clave_set = obtener_palabras_clave(worksheet_inicio)
        lista_negra = obtener_lista_negra(worksheet_lista_negra)
        rubros_y_productos = obtener_rubros_y_productos(worksheet_rubros)
        puntaje_clientes = obtener_puntaje_clientes(worksheet_clientes)
        ponderaciones = obtener_ponderaciones(worksheet_inicio)

        # Generar el ranking
        generar_ranking(
            worksheet_ranking,
            worksheet_ranking_no_relativo,
            worksheet_licitaciones_activas,
            palabras_clave_set,
            lista_negra,
            rubros_y_productos,
            puntaje_clientes,
            ponderaciones
        )

    except Exception as e:
        logging.error(f"Error en procesar_licitaciones_y_generar_ranking: {e}", exc_info=True)
        raise

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
    Genera el ranking de licitaciones y sube los resultados a Google Sheets.

    Args:
        worksheet_ranking (gspread.Worksheet): Hoja 2 para subir el ranking final.
        worksheet_ranking_no_relativo (gspread.Worksheet): Hoja 8 para subir puntajes no relativos.
        worksheet_licitaciones_activas (gspread.Worksheet): Hoja 7 con licitaciones activas.
        palabras_clave_set (set): Conjunto de palabras clave.
        lista_negra (set): Conjunto de frases de la lista negra.
        rubros_y_productos (dict): Diccionario mapeando rubros a productos.
        puntaje_clientes (dict): Diccionario mapeando clientes a puntajes.
        ponderaciones (dict): Diccionario con ponderaciones.
    """
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

        # Excluir organizaciones relacionadas con la salud
        regex_excluir = re.compile('|'.join(SALUD_EXCLUIR), re.IGNORECASE)
        df_licitaciones = df_licitaciones[~df_licitaciones['NombreOrganismo'].str.contains(regex_excluir, na=False)]
        logging.info(f"Filtradas licitaciones relacionadas con salud. Total: {len(df_licitaciones)}")

        # Agrupar por 'CodigoExterno'
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
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 'Puntaje Rubro', 
             'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ]

        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [[str(x) for x in row] for row in data_no_relativos]

        actualizar_hoja(worksheet_ranking_no_relativo, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 8 exitosamente.")

        # Seleccionar Top 100 licitaciones
        df_top_100 = df_licitaciones_agrupado.sort_values(
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
            df_top_100['Puntaje Relativo Rubro'] * ponderaciones['Puntaje Rubro'] +
            df_top_100['Puntaje Relativo Palabra'] * ponderaciones['Puntaje Palabra'] +
            df_top_100['Puntaje Relativo Monto'] * ponderaciones['Puntaje Monto'] +
            df_top_100['Puntaje Relativo Clientes'] * ponderaciones['Puntaje Clientes']
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
        data_final = [[str(x) if isinstance(x, str) else x for x in row] for row in data_final]

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
        logging.error(f"Error al generar el ranking: {e}", exc_info=True)
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

        # Recuperar todas las worksheets necesarias de una vez para minimizar las solicitudes
        try:
            worksheet_inicio = get_worksheet_with_retry(sh, 0)        # Hoja 1: Inicio
            worksheet_ranking = get_worksheet_with_retry(sh, 1)       # Hoja 2: Ranking
            worksheet_rubros = get_worksheet_with_retry(sh, 2)        # Hoja 3: Rubros
            worksheet_clientes = get_worksheet_with_retry(sh, 3)      # Hoja 4: Clientes
            worksheet_seleccion = get_worksheet_with_retry(sh, 5)     # Hoja 6: Seleccion
            worksheet_licitaciones_activas = get_worksheet_with_retry(sh, 6)  # Hoja 7: Licitaciones Activas y Duplicadas
            worksheet_ranking_no_relativo = get_worksheet_with_retry(sh, 7)  # Hoja 8: Ranking no relativo
            worksheet_lista_negra = get_worksheet_with_retry(sh, 9)   # Hoja 10: Lista Negra Palabras
            worksheet_sicep = get_worksheet_with_retry(sh, 10)        # Hoja 11: Licitaciones Sicep
        except Exception as e:
            logging.error(f"Error al obtener una o más hojas: {e}", exc_info=True)
            raise

        # Ejecutar el procesamiento principal
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

# -------------------------- Punto de Entrada --------------------------

if __name__ == "__main__":
    main()
