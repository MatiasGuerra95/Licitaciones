import pandas as pd
import requests
import os
import json
import re
from io import BytesIO
from zipfile import ZipFile
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from datetime import datetime
import logging
import time

# Configurar el registro (logging)
logging.basicConfig(
    level=logging.INFO,
    filename='your_script.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración de credenciales para Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_json:
    logging.error("La variable de entorno 'GOOGLE_APPLICATION_CREDENTIALS_JSON' no está definida.")
    raise EnvironmentError("La variable de entorno 'GOOGLE_APPLICATION_CREDENTIALS_JSON' no está definida.")

try:
    creds_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    logging.info("Autenticación con Google Sheets exitosa.")
except Exception as e:
    logging.error(f"Error al autenticar con Google Sheets: {e}")
    raise

# ID de la hoja de Google Sheets
sheet_id = '1EGoDJtO-b5dAGzC8LRYyZVdhHdcE2_ukgZAl-Ni9IxM'  # Reemplaza por el ID real de tu hoja de Google Sheets

# Función con retry para obtener hojas
@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def get_worksheet_with_retry(spreadsheet, index):
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
        logging.error(f"Error inesperado al obtener Hoja {index + 1}: {e}")
        raise

try:
    sh = gc.open_by_key(sheet_id)
    logging.info(f"Spreadsheet con ID {sheet_id} abierto exitosamente.")
except SpreadsheetNotFound as e:
    logging.error(f"Spreadsheet con ID {sheet_id} no encontrado: {e}")
    raise
except Exception as e:
    logging.error(f"Error al abrir Spreadsheet: {e}")
    raise

# Seleccionar las hojas necesarias con manejo de errores
try:
    worksheet_hoja1 = get_worksheet_with_retry(sh, 0)  # Hoja Inicio
    worksheet_hoja7 = get_worksheet_with_retry(sh, 6)  # Hoja Licitaciones Activas y Duplicadas
    worksheet_hoja6 = get_worksheet_with_retry(sh, 5)  # Hoja Seleccion
    worksheet_hoja10 = get_worksheet_with_retry(sh, 7)  # Hoja Ranking no relativo
    worksheet_hoja3 = get_worksheet_with_retry(sh, 2)  # Hoja Rubros
    worksheet_hoja2 = get_worksheet_with_retry(sh, 1)  # Hoja Ranking
    worksheet_hoja4 = get_worksheet_with_retry(sh, 3) #Hoja Clientes

except Exception as e:
    logging.error(f"Error al obtener una o más hojas: {e}")
    raise

# Extraer las fechas de las celdas C6 y C7
try:
    fecha_min_publicacion = worksheet_hoja1.acell('C6').value  # Fecha mínima de publicación
    fecha_min_cierre = worksheet_hoja1.acell('C7').value  # Fecha mínima de cierre
    logging.info(f"Fecha mínima de publicación: {fecha_min_publicacion}")
    logging.info(f"Fecha mínima de cierre: {fecha_min_cierre}")
except Exception as e:
    logging.error(f"Error al extraer fechas de la Hoja 1: {e}")
    raise

# Convertir las fechas extraídas a objetos datetime
try:
    fecha_min_publicacion = datetime.strptime(fecha_min_publicacion, '%Y-%m-%d')
    fecha_min_cierre = datetime.strptime(fecha_min_cierre, '%Y-%m-%d')
except ValueError as e:
    logging.error(f"Formato de fecha incorrecto: {e}")
    raise

# Obtener el mes y el año de la fecha mínima de publicación y cierre
mes_actual = fecha_min_publicacion.month
año_actual = fecha_min_publicacion.year

# Ajustar el mes y el año del mes anterior basado en la fecha mínima de publicación
if mes_actual == 1:  # Si es enero, el mes anterior es diciembre del año pasado
    mes_anterior = 12
    año_anterior = año_actual - 1
else:
    mes_anterior = mes_actual - 1
    año_anterior = año_actual

# URLs para descargar los archivos de licitaciones del mes en curso y del mes anterior
url_mes_actual = f"https://transparenciachc.blob.core.windows.net/lic-da/{año_actual}-{mes_actual}.zip"
url_mes_anterior = f"https://transparenciachc.blob.core.windows.net/lic-da/{año_anterior}-{mes_anterior}.zip"

logging.info(f"URL del mes actual: {url_mes_actual}")
logging.info(f"URL del mes anterior: {url_mes_anterior}")

# Función para descargar y procesar un archivo ZIP de licitaciones
def procesar_licitaciones(url):
    try:
        logging.info(f"Descargando licitaciones desde: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Asegurarse de que la solicitud fue exitosa
        zip_file = ZipFile(BytesIO(response.content))
        logging.info(f"Archivo ZIP descargado y abierto exitosamente desde: {url}")

        # Procesar cada archivo CSV en el ZIP
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
                        low_memory=False  # Para evitar DtypeWarning
                    )
                    df_list.append(df)
                    logging.info(f"Archivo {file_name} procesado exitosamente.")
                except Exception as e:
                    logging.error(f"Error procesando el archivo {file_name}: {e}")

        # Concatenar todos los DataFrames en uno solo si existen
        if df_list:
            df_concatenado = pd.concat(df_list, ignore_index=True)
            logging.info(f"Todos los archivos CSV de {url} han sido concatenados exitosamente.")
            return df_concatenado
        else:
            logging.warning(f"No se encontraron archivos CSV en {url}.")
            return pd.DataFrame()
    except requests.HTTPError as e:
        logging.error(f"Error HTTP al descargar {url}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error descargando o procesando el archivo desde {url}: {e}")
        return pd.DataFrame()

# Descargar y procesar los archivos de licitaciones del mes actual y el mes anterior
df_mes_actual = procesar_licitaciones(url_mes_actual)
df_mes_anterior = procesar_licitaciones(url_mes_anterior)

# Concatenar ambos DataFrames en uno solo
df_licitaciones = pd.concat([df_mes_actual, df_mes_anterior], ignore_index=True)
df_licitaciones['Nombre'] = df_licitaciones['Nombre'].str.lower()
df_licitaciones['Descripcion'] = df_licitaciones['Descripcion'].str.lower()

# Filtrar las licitaciones con 'CodigoEstado' = 5
if 'CodigoEstado' in df_licitaciones.columns:
    df_licitaciones = df_licitaciones[df_licitaciones['CodigoEstado'] == 5]
    logging.info(f"Filtradas licitaciones con 'CodigoEstado' = 5. Total: {len(df_licitaciones)}")
else:
    df_licitaciones = pd.DataFrame()  # Crear un DataFrame vacío en caso de error
    logging.warning("'CodigoEstado' no está en las columnas. Se creó un DataFrame vacío.")

# Seleccionar solo las columnas importantes, incluida la nueva columna 'FechaCreacion'
columnas_importantes = [
    'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCreacion', 'FechaCierre',
    'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico',
    'Tipo', 'CantidadReclamos', 'TiempoDuracionContrato', 'Link'
]
df_licitaciones = df_licitaciones[df_licitaciones.columns.intersection(columnas_importantes)]
logging.info(f"Seleccionadas columnas importantes. Total de licitaciones: {len(df_licitaciones)}")

# Filtrar las licitaciones por la FechaCreacion basadas en la fecha mínima de publicación
df_licitaciones['FechaCreacion'] = pd.to_datetime(df_licitaciones['FechaCreacion'], errors='coerce')
df_licitaciones = df_licitaciones[df_licitaciones['FechaCreacion'] >= fecha_min_publicacion]
logging.info(f"Filtradas licitaciones por 'FechaCreacion' >= {fecha_min_publicacion}. Total: {len(df_licitaciones)}")

# Filtrar licitaciones por FechaCierre
df_licitaciones['FechaCierre'] = pd.to_datetime(df_licitaciones['FechaCierre'], errors='coerce')
df_licitaciones = df_licitaciones[df_licitaciones['FechaCierre'] >= fecha_min_cierre]
logging.info(f"Filtradas licitaciones por 'FechaCierre' >= {fecha_min_cierre}. Total: {len(df_licitaciones)}")

# Convertir el DataFrame a una lista de listas para subirlo a Google Sheets
if not df_licitaciones.empty:
    data_to_upload = [df_licitaciones.columns.values.tolist()] + df_licitaciones.values.tolist()
    data_to_upload = [[str(x) for x in row] for row in data_to_upload]  # Convertir todos los valores a strings
    try:
        # Borrar el contenido actual de la Hoja 7 antes de actualizar
        worksheet_hoja7.clear()
        logging.info("Contenido de la Hoja 7 borrado exitosamente.")
        
        # Subir los datos a la Hoja 7 utilizando argumentos nombrados
        worksheet_hoja7.update(range_name='A1', values=data_to_upload)
        logging.info("Datos actualizados en Google Sheets exitosamente en la Hoja 7.")
    except APIError as e:
        logging.error(f"APIError al actualizar la Hoja 7: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al actualizar la Hoja 7: {e}")
        raise
else:
    logging.warning("No se procesaron licitaciones para subir a Google Sheets.")

# Función para eliminar licitaciones seleccionadas en la Hoja 6 desde la Hoja 7
def eliminar_licitaciones_seleccionadas():
    try:
        # Cargar las licitaciones seleccionadas de la Hoja 6 (usando el 'CodigoExterno')
        codigos_seleccionados = worksheet_hoja6.col_values(1)[3:]  # Asumiendo que los 'CodigoExterno' están en la primera columna
        logging.info(f"Total de 'CodigoExterno' seleccionados para eliminar: {len(codigos_seleccionados)}")
    
        # Cargar las licitaciones de la Hoja 7 (el mantenedor)
        licitaciones = worksheet_hoja7.get_all_values()
        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Total de licitaciones en la Hoja 7 antes de filtrar: {len(df_licitaciones)}")
    
        # Filtrar para eliminar las licitaciones que ya están en la Hoja 6 (según 'CodigoExterno')
        df_licitaciones_filtrado = df_licitaciones[~df_licitaciones['CodigoExterno'].isin(codigos_seleccionados)]
        logging.info(f"Total de licitaciones en la Hoja 7 después de filtrar: {len(df_licitaciones_filtrado)}")
    
        # Subir los datos filtrados nuevamente a la Hoja 7
        data_to_upload = [df_licitaciones_filtrado.columns.values.tolist()] + df_licitaciones_filtrado.values.tolist()
        data_to_upload = [[str(x) for x in row] for row in data_to_upload]  # Convertir todos los valores a strings
    
        worksheet_hoja7.clear()
        logging.info("Contenido de la Hoja 7 borrado antes de actualizar con datos filtrados.")
    
        worksheet_hoja7.update(range_name='A1', values=data_to_upload)
        logging.info(f"Se eliminaron {len(codigos_seleccionados)} licitaciones seleccionadas de la Hoja 7.")
    except APIError as e:
        logging.error(f"APIError al eliminar licitaciones seleccionadas: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al eliminar licitaciones seleccionadas: {e}")
        raise

# Ejecutar la función para eliminar las licitaciones seleccionadas
eliminar_licitaciones_seleccionadas()

# Función para obtener las ponderaciones desde la Hoja 1
def obtener_ponderaciones():
    try:
        ponderaciones = {
            'Puntaje Rubro': float(worksheet_hoja1.acell('K11').value.strip('%')) / 100,  # Remove '%' and divide by 100
            'Puntaje Palabra': float(worksheet_hoja1.acell('K25').value.strip('%')) / 100,  # Remove '%' and divide by 100
            'Puntaje Clientes': float(worksheet_hoja1.acell('K39').value.strip('%')) / 100,  # Remove '%' and divide by 100
            'Puntaje Monto': float(worksheet_hoja1.acell('K43').value.strip('%')) / 100  # Remove '%' and divide by 100
        }
        logging.info(f"Ponderaciones obtenidas: {ponderaciones}")
        return ponderaciones
    except Exception as e:
        logging.error(f"Error al obtener ponderaciones: {e}")
        raise

# Función para obtener las palabras clave desde la Hoja 1
def obtener_palabras_clave():
    try:
        palabras = [
            worksheet_hoja1.acell('C27').value, worksheet_hoja1.acell('C28').value,
            worksheet_hoja1.acell('C29').value, worksheet_hoja1.acell('C30').value,
            worksheet_hoja1.acell('C31').value, worksheet_hoja1.acell('C32').value,
            worksheet_hoja1.acell('C33').value,
            worksheet_hoja1.acell('F27').value, worksheet_hoja1.acell('F28').value,
            worksheet_hoja1.acell('F29').value, worksheet_hoja1.acell('F30').value,
            worksheet_hoja1.acell('F31').value, worksheet_hoja1.acell('F32').value,
            worksheet_hoja1.acell('F33').value, worksheet_hoja1.acell('F34').value,
            worksheet_hoja1.acell('F35').value,
            worksheet_hoja1.acell('I27').value, worksheet_hoja1.acell('I28').value,
            worksheet_hoja1.acell('I29').value, worksheet_hoja1.acell('I30').value,
            worksheet_hoja1.acell('I31').value, worksheet_hoja1.acell('I32').value,
            worksheet_hoja1.acell('I33').value, worksheet_hoja1.acell('I34').value
        ]
        palabras_clave = [p.lower() for p in palabras if p]
        logging.info(f"Palabras clave obtenidas: {palabras_clave}")
        return palabras_clave
    except Exception as e:
        logging.error(f"Error al obtener palabras clave: {e}")
        raise

# Función para obtener rubros y productos desde la Hoja 1
def obtener_rubros_y_productos():
    try:
        rubros_y_productos = {}
    
        # Extraer rubros
        rubros = {
            'rubro1': worksheet_hoja1.acell('C13').value,
            'rubro2': worksheet_hoja1.acell('F13').value,
            'rubro3': worksheet_hoja1.acell('I13').value,
        }
    
        # Extraer productos correspondientes a cada rubro
        productos = {
            'rubro1': [worksheet_hoja1.acell(f'C{row}').value for row in range(14, 24)],
            'rubro2': [worksheet_hoja1.acell(f'F{row}').value for row in range(14, 24)],
            'rubro3': [worksheet_hoja1.acell(f'I{row}').value for row in range(14, 24)]
        }
    
        # Crear el diccionario asociando cada rubro con sus productos no vacíos
        for key, rubro in rubros.items():
            if rubro:
                productos_rubro = [producto for producto in productos[key] if producto]  # Filtrar productos no vacíos
                rubros_y_productos[rubro.lower()] = [producto.lower() for producto in productos_rubro]  # Guardar en minúsculas
        logging.info(f"Rubros y productos obtenidos: {rubros_y_productos}")
        return rubros_y_productos
    except Exception as e:
        logging.error(f"Error al obtener rubros y productos: {e}")
        raise

# Función para obtener los clientes y su estado desde la Hoja 3
def obtener_puntaje_clientes():
    try:
        # Asumiendo que los nombres de clientes están en la columna D y los estados en la columna E
        clientes = worksheet_hoja4.col_values(4)[3:]  # D4 hacia abajo (nombres de los clientes)
        estados = worksheet_hoja4.col_values(5)[3:]  # E4 hacia abajo (estados: "vigente" o "no vigente")
    
        # Crear un diccionario {cliente: estado}
        puntaje_clientes = {}
    
        for cliente, estado in zip(clientes, estados):
            estado_lower = estado.strip().lower()
            if estado_lower == 'vigente':
                puntaje_clientes[cliente.lower()] = 10  # +10 si es vigente
            elif estado_lower == 'no vigente':
                puntaje_clientes[cliente.lower()] = 5  # +5 si no vigente
            else:
                puntaje_clientes[cliente.lower()] = 0  # Puntaje 0 si no está claro el estado
    
        logging.info(f"Puntaje de clientes obtenidos: {puntaje_clientes}")
        return puntaje_clientes
    except Exception as e:
        logging.error(f"Error al obtener puntaje de clientes: {e}")
        raise

# Función para calcular el puntaje por palabras clave
def calcular_puntaje_palabra(nombre, descripcion, palabras_clave):
    puntaje_palabra = 0
    texto = f"{nombre.lower()} {descripcion.lower()}"
    palabras_texto = set(re.findall(r'\b\w+\b', texto))
    for palabra_clave in palabras_clave:
        if palabra_clave in palabras_texto:
            puntaje_palabra += 10
    return puntaje_palabra

# Función para calcular el puntaje por rubros y productos
def calcular_puntaje_rubro(row, rubros_y_productos):
    rubro_column = row['Rubro3'].lower() if pd.notnull(row['Rubro3']) else ''
    productos_column = row['Nombre producto genrico'].lower() if pd.notnull(row['Nombre producto genrico']) else ''
    puntaje_rubro = 0
    rubros_presentes = set()
    productos_presentes = set()

    # Buscar rubros presentes en la columna "Rubro3"
    for rubro, productos in rubros_y_productos.items():
        if rubro in rubro_column:
            rubros_presentes.add(rubro)  # Añadir rubro al set
            # Acumular los productos asociados al rubro
            for producto in productos:
                if producto in productos_column:
                    productos_presentes.add(producto)

    # Asignar 5 puntos por cada rubro encontrado
    puntaje_rubro += len(rubros_presentes) * 5

    # Asignar 10 puntos por cada producto encontrado
    puntaje_rubro += len(productos_presentes) * 10

    return puntaje_rubro

# Función para calcular el puntaje basado en el monto (usando 'TiempoDuracionContrato')
def calcular_puntaje_monto(tipo_licitacion, tiempo_duracion_contrato):
    # Diccionario que mapea los tipos de licitaciones con su monto aproximado
    montos_por_tipo = {
        'L1': 0, 'LE': 100, 'LP': 1000, 'LQ': 2000, 'LR': 5000, 'LS': 0,
        'E2': 0, 'CO': 100, 'B2': 1000, 'H2': 2000, 'I2': 5000
    }

    tipo_licitacion = tipo_licitacion.strip().upper()
    monto_base = montos_por_tipo.get(tipo_licitacion, 0)

    # Evitar división por cero
    try:
        tiempo_duracion = float(tiempo_duracion_contrato)
        if tiempo_duracion > 0:
            return monto_base / tiempo_duracion
        else:
            return 0
    except ValueError:
        return 0

# Función para calcular el puntaje basado en los clientes
def calcular_puntaje_clientes(nombre_organismo, puntaje_clientes):
    return puntaje_clientes.get(nombre_organismo.lower(), 0)  # Si no está en el mantenedor, devuelve 0

# Función para actualizar una hoja con manejo de errores y logging
@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def actualizar_hoja(worksheet, rango, datos):
    try:
        worksheet.update(range_name=rango, values=datos)
        logging.info(f"Hoja actualizada exitosamente en el rango {rango}.")
    except APIError as e:
        logging.warning(f"APIError al actualizar la Hoja en el rango {rango}: {e}. Reintentando...")
        raise
    except Exception as e:
        logging.error(f"Error al actualizar la Hoja en el rango {rango}: {e}")
        raise

# Función para procesar las licitaciones y generar un ranking ajustado para que los puntajes relativos sumen 100
def procesar_licitaciones_y_generar_ranking():
    try:
        # Obtener las palabras clave, rubros-productos y ponderaciones
        palabras_clave = obtener_palabras_clave()
        rubros_y_productos = obtener_rubros_y_productos()
        puntaje_clientes = obtener_puntaje_clientes()
        ponderaciones = obtener_ponderaciones()
    
        # Cargar las licitaciones desde la Hoja 7 (el mantenedor)
        licitaciones = worksheet_hoja7.get_all_values()
        df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])
        logging.info(f"Licitaciones cargadas desde la Hoja 7. Total: {len(df_licitaciones)}")
    
        # Filtrar licitaciones cuyo 'TiempoDuracionContrato' no sea 0
        df_licitaciones = df_licitaciones[df_licitaciones['TiempoDuracionContrato'] != '0']
        logging.info(f"Filtradas licitaciones con 'TiempoDuracionContrato' != 0. Total: {len(df_licitaciones)}")
    
        # Filtrar para eliminar licitaciones relacionadas con la salud en el NombreOrganismo
        salud_excluir = [
            'CENTRO DE SALUD', 'PREHOSPITALARIA', 'REFERENCIA DE SALUD',
            'REFERENCIAL DE SALUD', 'ONCOLOGICO', 'CESFAM', 'COMPLEJO ASISTENCIAL',
            'CONSULTORIO', 'CRS', 'HOSPITAL', 'INSTITUTO DE NEUROCIRUGÍA',
            'INSTITUTO DE SALUD PÚBLICA DE CHILE', 'INSTITUTO NACIONAL DE GERIATRIA',
            'INSTITUTO NACIONAL DE REHABILITACION', 'INSTITUTO NACIONAL DEL CANCER',
            'INSTITUTO NACIONAL DEL TORAX', 'INSTITUTO PSIQUIÁTRICO',
            'SERV NAC SALUD', 'SERV SALUD', 'SERVICIO DE SALUD',
            'SERVICIO NACIONAL DE SALUD', 'SERVICIO SALUD'
        ]
    
        regex_excluir = '|'.join(salud_excluir)
        df_licitaciones = df_licitaciones[~df_licitaciones['NombreOrganismo'].str.contains(regex_excluir, case=False, na=False)]
        logging.info(f"Filtradas licitaciones relacionadas con salud. Total: {len(df_licitaciones)}")
    
        # Agrupar por 'CodigoExterno' para combinar rubros, productos, etc.
        df_licitaciones_agrupado = df_licitaciones.groupby('CodigoExterno').agg({
            'Nombre': 'first',
            'NombreOrganismo': 'first',
            'Link': 'first',
            'Rubro3': lambda x: ' '.join(x),  # Concatenar rubros
            'Nombre producto genrico': lambda x: ' '.join(x),  # Concatenar productos
            'Tipo': 'first',
            'CantidadReclamos': 'first',
            'Descripcion': 'first',
            'TiempoDuracionContrato': 'first'
        }).reset_index()
        logging.info(f"Licitaciones agrupadas por 'CodigoExterno'. Total: {len(df_licitaciones_agrupado)}")
    
        # Calcular el puntaje por palabras y rubros-productos
        df_licitaciones_agrupado['Puntaje Palabra'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_palabra(row['Nombre'], row['Descripcion'], palabras_clave),
            axis=1
        )
        logging.info("Puntaje por palabras clave calculado.")
    
        df_licitaciones_agrupado['Puntaje Rubro'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_rubro(row, rubros_y_productos),
            axis=1
        )
        logging.info("Puntaje por rubros calculado.")
    
        # Calcular el puntaje basado en el monto de las licitaciones usando 'TiempoDuracionContrato'
        df_licitaciones_agrupado['Puntaje Monto'] = df_licitaciones_agrupado.apply(
            lambda row: calcular_puntaje_monto(row['Tipo'], row['TiempoDuracionContrato']),
            axis=1
        )
        logging.info("Puntaje por monto calculado.")
    
        # Calcular el puntaje basado en los clientes
        df_licitaciones_agrupado['Puntaje Clientes'] = df_licitaciones_agrupado['NombreOrganismo'].apply(
            lambda cliente: calcular_puntaje_clientes(cliente, puntaje_clientes)
        )
        logging.info("Puntaje por clientes calculado.")
    
        # Calcular 'Puntaje Total'
        df_licitaciones_agrupado['Puntaje Total'] = (
            df_licitaciones_agrupado['Puntaje Rubro'] +
            df_licitaciones_agrupado['Puntaje Palabra'] +
            df_licitaciones_agrupado['Puntaje Monto'] +
            df_licitaciones_agrupado['Puntaje Clientes']
        )
        logging.info("Puntaje total calculado.")
    
        # Guardar los puntajes NO relativos en una hoja aparte (Hoja 10)
        df_no_relativos = df_licitaciones_agrupado[
            ['CodigoExterno', 'Nombre', 'NombreOrganismo', 'Puntaje Rubro', 
             'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']
        ]
    
        data_no_relativos = [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist()
        data_no_relativos = [[str(x) for x in row] for row in data_no_relativos]
    
        actualizar_hoja(worksheet_hoja10, 'A1', data_no_relativos)
        logging.info("Puntajes no relativos subidos a la Hoja 10 exitosamente.")
    
        # Ordenar las licitaciones por 'Puntaje Total' para seleccionar las 100 mejores
        df_top_100 = df_licitaciones_agrupado.sort_values(
            by=['Puntaje Rubro', 'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes'], 
            ascending=False
        ).head(100)
        logging.info(f"Top 100 licitaciones seleccionadas.")
    
        # Calcular los totales para cada criterio dentro del Top 100
        total_rubro_top100 = df_top_100['Puntaje Rubro'].sum()
        total_palabra_top100 = df_top_100['Puntaje Palabra'].sum()
        total_monto_top100 = df_top_100['Puntaje Monto'].sum()
        total_clientes_top100 = df_top_100['Puntaje Clientes'].sum()
        logging.info("Totales calculados para cada criterio dentro del Top 100.")
    
        # Ajustar los puntajes relativos para que sumen 100 dentro del Top 100
        df_top_100['Puntaje Relativo Rubro'] = (df_top_100['Puntaje Rubro'] / total_rubro_top100 * 100) if total_rubro_top100 > 0 else 0
        df_top_100['Puntaje Relativo Palabra'] = (df_top_100['Puntaje Palabra'] / total_palabra_top100 * 100) if total_palabra_top100 > 0 else 0
        df_top_100['Puntaje Relativo Monto'] = (df_top_100['Puntaje Monto'] / total_monto_top100 * 100) if total_monto_top100 > 0 else 0
        df_top_100['Puntaje Relativo Clientes'] = (df_top_100['Puntaje Clientes'] / total_clientes_top100 * 100) if total_clientes_top100 > 0 else 0
        logging.info("Puntajes relativos ajustados para que sumen 100.")
    
        # Crear una nueva columna 'Puntaje Total SUMAPRODUCTO' usando SUMAPRODUCTO de puntajes relativos y las ponderaciones
        df_top_100['Puntaje Total SUMAPRODUCTO'] = (
            df_top_100['Puntaje Relativo Rubro'] * ponderaciones['Puntaje Rubro'] +
            df_top_100['Puntaje Relativo Palabra'] * ponderaciones['Puntaje Palabra'] +
            df_top_100['Puntaje Relativo Monto'] * ponderaciones['Puntaje Monto'] +
            df_top_100['Puntaje Relativo Clientes'] * ponderaciones['Puntaje Clientes']
        )
        logging.info("Puntaje Total SUMAPRODUCTO calculado.")
    
        # Ordenar las licitaciones por 'Puntaje Total SUMAPRODUCTO' de mayor a menor
        df_top_100 = df_top_100.sort_values(by='Puntaje Total SUMAPRODUCTO', ascending=False)
        logging.info("Top 100 licitaciones ordenadas por 'Puntaje Total SUMAPRODUCTO'.")
    
        # Crear la estructura para la Hoja 2 con las columnas especificadas
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
    
        data_final = [df_final.columns.values.tolist()] + df_final.values.tolist()
        data_final = [[str(x) for x in row] for row in data_final]  # Convertir todos los valores a strings
    
        # Obtener el valor de la celda A1 para preservarlo
        nombre_a1 = worksheet_hoja2.acell('A1').value

        # Limpiar la hoja pero mantener el valor de A1
        worksheet_hoja2.clear()
        worksheet_hoja2.update('A1', [[nombre_a1]])  # Restaurar el valor original de A1


        actualizar_hoja(worksheet_hoja2, 'A3', data_final)
        logging.info("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")
    
    except Exception as e:
        logging.error(f"Error en procesar_licitaciones_y_generar_ranking: {e}")
        raise

# Ejecutar la función principal
procesar_licitaciones_y_generar_ranking()
