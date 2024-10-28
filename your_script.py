import pandas as pd
import requests
import os
import json
import re
from io import BytesIO
from zipfile import ZipFile
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime


# Configuración de credenciales para Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
creds_info = json.loads(creds_json)
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc = gspread.authorize(creds)

# ID de la hoja de Google Sheets
sheet_id = '1EGoDJtO-b5dAGzC8LRYyZVdhHdcE2_ukgZAl-Ni9IxM'  # Reemplaza por el ID real de tu hoja de Google Sheets
sh = gc.open_by_key(sheet_id)

# Seleccionar la Hoja 1 para extraer fechas de C6 y C7
worksheet_hoja1 = sh.get_worksheet(0)

# Extraer las fechas de las celdas C6 y C7
fecha_min_publicacion = worksheet_hoja1.acell('C6').value  # Fecha mínima de publicación
fecha_min_cierre = worksheet_hoja1.acell('C7').value  # Fecha mínima de cierre

# Convertir las fechas extraídas a objetos datetime
fecha_min_publicacion = datetime.strptime(fecha_min_publicacion, '%Y-%m-%d')
fecha_min_cierre = datetime.strptime(fecha_min_cierre, '%Y-%m-%d')

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

# Función para descargar y procesar un archivo ZIP de licitaciones
def procesar_licitaciones(url):
    try:
        # Descargar el archivo ZIP
        response = requests.get(url)
        zip_file = ZipFile(BytesIO(response.content))

        # Procesar cada archivo CSV en el ZIP
        df_list = []
        for file_name in zip_file.namelist():
            if file_name.endswith('.csv'):
                print(f"Procesando {file_name}...")
                try:
                    df = pd.read_csv(zip_file.open(file_name), encoding='ISO-8859-1', sep=';', on_bad_lines='skip')
                    df_list.append(df)
                except Exception as e:
                    print(f"Error procesando el archivo {file_name}: {e}")

        # Concatenar todos los DataFrames en uno solo si existen
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
    except Exception as e:
        print(f"Error descargando o procesando el archivo: {e}")
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
else:
    df_licitaciones = pd.DataFrame()  # Crear un DataFrame vacío en caso de error

# Seleccionar solo las columnas importantes, incluida la nueva columna 'FechaCreacion'
columnas_importantes = ['CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCreacion', 'FechaCierre', 'Descripcion', 'NombreOrganismo', 'Rubro3', 'Nombre producto genrico', 'Tipo', 'CantidadReclamos', 'TiempoDuracionContrato', 'Link']
df_licitaciones = df_licitaciones[df_licitaciones.columns.intersection(columnas_importantes)]

# Filtrar las licitaciones por la FechaCreacion basadas en la fecha mínima de publicación
df_licitaciones['FechaCreacion'] = pd.to_datetime(df_licitaciones['FechaCreacion'], errors='coerce')
df_licitaciones = df_licitaciones[df_licitaciones['FechaCreacion'] >= fecha_min_publicacion]

# Filtrar licitaciones por FechaCierre
df_licitaciones['FechaCierre'] = pd.to_datetime(df_licitaciones['FechaCierre'], errors='coerce')
df_licitaciones = df_licitaciones[df_licitaciones['FechaCierre'] >= fecha_min_cierre]

# Seleccionar la Hoja 7 donde se actualizarán los datos
worksheet = sh.get_worksheet(6)  # Hoja 7 (el índice es base 0, por lo que la Hoja 7 es el índice 6)

# Convertir el DataFrame a una lista de listas para subirlo a Google Sheets
if not df_licitaciones.empty:
    data_to_upload = [df_licitaciones.columns.values.tolist()] + df_licitaciones.values.tolist()
    data_to_upload = [[str(x) for x in row] for row in data_to_upload]  # Convertir todos los valores a strings

    # Borrar el contenido actual de la Hoja 7 antes de actualizar
    worksheet.clear()

    # Subir los datos a la Hoja 7
    worksheet.update('A1', data_to_upload)

    print("Datos actualizados en Google Sheets exitosamente.")
else:
    print("No se procesaron licitaciones para subir a Google Sheets.")

# Función para eliminar licitaciones seleccionadas en la Hoja 6 desde la Hoja 7
def eliminar_licitaciones_seleccionadas():
    # Cargar las licitaciones seleccionadas de la Hoja 6 (usando el 'CodigoExterno')
    worksheet_hoja6 = sh.get_worksheet(5)  # Hoja 6
    codigos_seleccionados = worksheet_hoja6.col_values(1)[3:]  # Asumiendo que los 'CodigoExterno' están en la primera columna

    # Cargar las licitaciones de la Hoja 7 (el mantenedor)
    worksheet_hoja7 = sh.get_worksheet(6)  # Hoja 7
    licitaciones = worksheet_hoja7.get_all_values()
    df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])

    # Filtrar para eliminar las licitaciones que ya están en la Hoja 6 (según 'CodigoExterno')
    df_licitaciones_filtrado = df_licitaciones[~df_licitaciones['CodigoExterno'].isin(codigos_seleccionados)]

    # Subir los datos filtrados nuevamente a la Hoja 7
    worksheet_hoja7.clear()  # Limpiar la Hoja 7 antes de la actualización
    worksheet_hoja7.update('A1', [df_licitaciones_filtrado.columns.values.tolist()] + df_licitaciones_filtrado.values.tolist())

    print(f"Se eliminaron {len(codigos_seleccionados)} licitaciones seleccionadas de la Hoja 7.")

# Ejecutar la función para eliminar las licitaciones seleccionadas
eliminar_licitaciones_seleccionadas()

# Función para obtener las ponderaciones desde la Hoja 1
def obtener_ponderaciones():
    worksheet_hoja1 = sh.get_worksheet(0)

    ponderaciones = {
        'Puntaje Rubro': float(worksheet_hoja1.acell('K11').value.strip('%')) / 100,  # Remove '%' and divide by 100
        'Puntaje Palabra': float(worksheet_hoja1.acell('K25').value.strip('%')) / 100,  # Remove '%' and divide by 100
        'Puntaje Clientes': float(worksheet_hoja1.acell('K39').value.strip('%')) / 100,  # Remove '%' and divide by 100
        'Puntaje Monto': float(worksheet_hoja1.acell('K43').value.strip('%')) / 100  # Remove '%' and divide by 100
    }

    return ponderaciones

# Función para obtener las palabras clave desde la Hoja 1
def obtener_palabras_clave():
    worksheet_hoja1 = sh.get_worksheet(0)
    palabras = [
        worksheet_hoja1.acell('C27').value, worksheet_hoja1.acell('C28').value, worksheet_hoja1.acell('C29').value, worksheet_hoja1.acell('C30').value, worksheet_hoja1.acell('C31').value,
        worksheet_hoja1.acell('F27').value, worksheet_hoja1.acell('F28').value, worksheet_hoja1.acell('F29').value, worksheet_hoja1.acell('F30').value, worksheet_hoja1.acell('F31').value,
        worksheet_hoja1.acell('I27').value, worksheet_hoja1.acell('I28').value, worksheet_hoja1.acell('I29').value, worksheet_hoja1.acell('I30').value
    ]
    return [p.lower() for p in palabras if p]  # Filtrar palabras no vacías y convertir a minúsculas

# Función para obtener rubros y productos desde la Hoja 1
def obtener_rubros_y_productos():
    worksheet_hoja1 = sh.get_worksheet(0)
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

    return rubros_y_productos

# Función para obtener los clientes y su estado desde la Hoja 3
def obtener_puntaje_clientes():
    worksheet_hoja3 = sh.get_worksheet(2)  # Hoja 3
    # Asumiendo que los nombres de clientes están en la columna D y los estados en la columna E
    clientes = worksheet_hoja3.col_values(4)[3:]  # D4 hacia abajo (nombres de los clientes)
    estados = worksheet_hoja3.col_values(5)[3:]  # E4 hacia abajo (estados: "vigente" o "no vigente")

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

    return puntaje_clientes

# Función para calcular el puntaje por palabras clave
def calcular_puntaje_palabra(nombre, descripcion, palabras_clave):
    puntaje_palabra = 0
    texto = f"{nombre.lower()} {descripcion.lower()}"
    palabras_texto = re.findall(r'\b\w+\b', texto)
    for palabra_clave in palabras_clave:
        if palabra_clave in palabras_texto:
            puntaje_palabra += 10
            continue
    return puntaje_palabra

# Función para calcular el puntaje por rubros y productos, ahora acumulando correctamente los rubros y productos
def calcular_puntaje_rubro(row, rubros_y_productos):
    rubro_column = row['Rubro3']
    productos_column = row['Nombre producto genrico']
    puntaje_rubro = 0
    rubros_presentes = set()
    productos_presentes = set()

    # Buscar rubros presentes en la columna "Rubro3"
    for rubro, productos in rubros_y_productos.items():
        if rubro in rubro_column.lower():
            rubros_presentes.add(rubro)  # Añadir rubro al set

            # Acumular los productos asociados al rubro
            for producto in productos:
                if producto in productos_column.lower():
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

# Función para procesar las licitaciones y generar un ranking ajustado para que los puntajes relativos sumen 100
def procesar_licitaciones_y_generar_ranking():
    # Obtener las palabras clave, rubros-productos y ponderaciones
    palabras_clave = obtener_palabras_clave()
    rubros_y_productos = obtener_rubros_y_productos()
    puntaje_clientes = obtener_puntaje_clientes()
    ponderaciones = obtener_ponderaciones()

    # Cargar las licitaciones desde la Hoja 7 (el mantenedor)
    worksheet_hoja7 = sh.get_worksheet(6)  # Hoja 7
    licitaciones = worksheet_hoja7.get_all_values()
    df_licitaciones = pd.DataFrame(licitaciones[1:], columns=licitaciones[0])

    # Filtrar licitaciones cuyo 'TiempoDuracionContrato' no sea 0
    df_licitaciones = df_licitaciones[df_licitaciones['TiempoDuracionContrato'] != '0']

    # Filtrar para eliminar licitaciones relacionadas con la salud en el NombreOrganismo
    salud_excluir = [
                    'CENTRO DE SALUD',
                    'PREHOSPITALARIA',
                    'REFERENCIA DE SALUD',
                    'REFERENCIAL DE SALUD',
                    'ONCOLOGICO',
                    'CESFAM',
                    'COMPLEJO ASISTENCIAL',
                    'CONSULTORIO',
                    'CRS',
                    'HOSPITAL',
                    'INSTITUTO DE NEUROCIRUGÍA',
                    'INSTITUTO DE SALUD PÚBLICA DE CHILE',
                    'INSTITUTO NACIONAL DE GERIATRIA',
                    'INSTITUTO NACIONAL DE REHABILITACION',
                    'INSTITUTO NACIONAL DEL CANCER',
                    'INSTITUTO NACIONAL DEL TORAX',
                    'INSTITUTO PSIQUIÁTRICO',
                    'SERV NAC SALUD',
                    'SERV SALUD',
                    'SERVICIO DE SALUD',
                    'SERVICIO NACIONAL DE SALUD',
                    'SERVICIO SALUD'
                    ]

    regex_excluir = '|'.join(salud_excluir)
    df_licitaciones = df_licitaciones[~df_licitaciones['NombreOrganismo'].str.contains(regex_excluir, case=False, na=False)]

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

    # Calcular el puntaje por palabras y rubros-productos
    df_licitaciones_agrupado['Puntaje Palabra'] = df_licitaciones_agrupado.apply(
        lambda row: calcular_puntaje_palabra(row['Nombre'], row['Descripcion'], palabras_clave), axis=1
    )

    df_licitaciones_agrupado['Puntaje Rubro'] = df_licitaciones_agrupado.apply(
        lambda row: calcular_puntaje_rubro(row, rubros_y_productos), axis=1
    )

    # Calcular el puntaje basado en el monto de las licitaciones usando 'TiempoDuracionContrato'
    df_licitaciones_agrupado['Puntaje Monto'] = df_licitaciones_agrupado.apply(
        lambda row: calcular_puntaje_monto(row['Tipo'], row['TiempoDuracionContrato']), axis=1
    )

    # Calcular el puntaje basado en los clientes
    df_licitaciones_agrupado['Puntaje Clientes'] = df_licitaciones_agrupado['NombreOrganismo'].apply(
        lambda cliente: calcular_puntaje_clientes(cliente, puntaje_clientes)
    )

     # Calculate 'Puntaje Total' before selecting it
    df_licitaciones_agrupado['Puntaje Total'] = (
        df_licitaciones_agrupado['Puntaje Rubro'] +
        df_licitaciones_agrupado['Puntaje Palabra'] +
        df_licitaciones_agrupado['Puntaje Monto'] +
        df_licitaciones_agrupado['Puntaje Clientes']
    )

    # Guardar los puntajes NO relativos en una hoja aparte (Hoja 10)
    df_no_relativos = df_licitaciones_agrupado[['CodigoExterno','Nombre', 'NombreOrganismo', 'Puntaje Rubro', 'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes', 'Puntaje Total']]

    # Cargar los datos en la Hoja 10
    worksheet_hoja10 = sh.get_worksheet(9)  # Hoja 10
    worksheet_hoja10.clear()
    worksheet_hoja10.update('A1', [df_no_relativos.columns.values.tolist()] + df_no_relativos.values.tolist())

    print("Puntajes no relativos subidos a la Hoja 10 exitosamente.")


    # Ordenar las licitaciones por 'Puntaje Total' para seleccionar las 100 mejores
    df_top_100 = df_licitaciones_agrupado.sort_values(by=['Puntaje Rubro', 'Puntaje Palabra', 'Puntaje Monto', 'Puntaje Clientes'], ascending=False).head(100)

    # Calcular los totales para cada criterio dentro del Top 100
    total_rubro_top100 = df_top_100['Puntaje Rubro'].sum()
    total_palabra_top100 = df_top_100['Puntaje Palabra'].sum()
    total_monto_top100 = df_top_100['Puntaje Monto'].sum()
    total_clientes_top100 = df_top_100['Puntaje Clientes'].sum()

    # Ajustar los puntajes relativos para que sumen 100 dentro del Top 100
    df_top_100['Puntaje Relativo Rubro'] = (df_top_100['Puntaje Rubro'] / total_rubro_top100 * 100) if total_rubro_top100 > 0 else 0
    df_top_100['Puntaje Relativo Palabra'] = (df_top_100['Puntaje Palabra'] / total_palabra_top100 * 100) if total_palabra_top100 > 0 else 0
    df_top_100['Puntaje Relativo Monto'] = (df_top_100['Puntaje Monto'] / total_monto_top100 * 100) if total_monto_top100 > 0 else 0
    df_top_100['Puntaje Relativo Clientes'] = (df_top_100['Puntaje Clientes'] / total_clientes_top100 * 100) if total_clientes_top100 > 0 else 0

    # Crear una nueva columna 'Puntaje Total SUMAPRODUCTO' usando SUMAPRODUCTO de puntajes relativos y las ponderaciones
    df_top_100['Puntaje Total SUMAPRODUCTO'] = (
        df_top_100['Puntaje Relativo Rubro'] * ponderaciones['Puntaje Rubro'] +
        df_top_100['Puntaje Relativo Palabra'] * ponderaciones['Puntaje Palabra'] +
        df_top_100['Puntaje Relativo Monto'] * ponderaciones['Puntaje Monto'] +
        df_top_100['Puntaje Relativo Clientes'] * ponderaciones['Puntaje Clientes']
    )

    # Ordenar las licitaciones por 'Puntaje Total SUMAPRODUCTO' de mayor a menor
    df_top_100 = df_top_100.sort_values(by='Puntaje Total SUMAPRODUCTO', ascending=False)

    # Crear la estructura para la Hoja 2 con las columnas especificadas
    df_top_100['#'] = range(1, len(df_top_100) + 1)

    df_top_100 = df_top_100.rename(columns={
        'Puntaje Relativo Rubro': 'Rubro',
        'Puntaje Relativo Palabra': 'Palabra',
        'Puntaje Relativo Monto': 'Monto',
        'Puntaje Relativo Clientes': 'Clientes',
        'Puntaje Total SUMAPRODUCTO': 'Puntaje Final'
    })

    df_final = df_top_100[['#','CodigoExterno', 'Nombre', 'NombreOrganismo', 'Link', 'Rubro', 'Palabra',
                           'Monto', 'Clientes', 'Puntaje Final']]

    # Cargar los datos en la Hoja 2
    worksheet_hoja2 = sh.get_worksheet(1)  # Hoja 2
    worksheet_hoja2.clear()
    worksheet_hoja2.update('A3', [df_final.columns.values.tolist()] + df_final.values.tolist())

    print("Nuevo ranking de licitaciones con puntajes ajustados subido a la Hoja 2 exitosamente.")

# Ejecutar la función principal
procesar_licitaciones_y_generar_ranking()
