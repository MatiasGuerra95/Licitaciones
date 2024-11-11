# nuevo_portal.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import pandas as pd
import time
import os

load_dotenv()

# Configuración de usuario y contraseña para la nueva plataforma
USER = os.getenv("PORTAL_USER")
PASSWORD = os.getenv("PORTAL_PASSWORD")

def setup_driver():
    options = Options()
    # options.add_argument("--headless")  # Puedes comentar esta línea si quieres ver el navegador
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def acceder_nuevas_licitaciones(driver):
    try:
        menu_licitaciones = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "menu_nuevas_licitaciones"))
        )
        driver.execute_script("arguments[0].click();", menu_licitaciones)
        print("Menú 'Licitaciones' abierto.")

        info_licitaciones = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "menu_nuevas_publicaciones_mandante"))
        )
        driver.execute_script("arguments[0].click();", info_licitaciones)
        print("Accedido a 'Información de Licitaciones'.")
    except Exception as e:
        print(f"Error al acceder a 'Información de Licitaciones': {e}")

def omitir_finalizados(driver):
    try:
        modals = driver.find_elements(By.CLASS_NAME, "b-modal")
        for modal in modals:
            driver.execute_script("arguments[0].style.display = 'none';", modal)
            print("Modal oculto para acceder al checkbox.")

        checkbox_finalizados = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "chxFinalizados"))
        )
        if not checkbox_finalizados.is_selected():
            checkbox_finalizados.click()
            print("Checkbox de finalizados marcado correctamente.")
        else:
            print("Checkbox de finalizados ya estaba marcado.")
    except Exception as e:
        print(f"No se pudo marcar el checkbox de finalizados: {e}")

def obtener_licitaciones_disponibles(driver, licitaciones_visitadas):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[onclick^='entrarPublicacion']"))
        )
        
        licitaciones_links = []
        licitaciones_elementos = driver.find_elements(By.CSS_SELECTOR, "a[onclick^='entrarPublicacion']")
        for elemento in licitaciones_elementos:
            licitacion_id = elemento.get_attribute("name")
            if licitacion_id not in licitaciones_visitadas:
                licitaciones_links.append((licitacion_id, elemento))
        print(f"Se encontraron {len(licitaciones_links)} nuevas licitaciones disponibles.")
        return licitaciones_links
    except Exception as e:
        print(f"Error al obtener enlaces de licitaciones: {e}")
        return []

def extraer_detalles_licitacion(driver):
    """Extrae los detalles de una licitación y el enlace de acceso directo."""
    try:
        # Extraer la URL actual como enlace de la licitación
        url_licitacion = driver.current_url

        # Extraer título y descripción
        titulo = driver.find_element(By.ID, "lblTituloDetallePublicacion").text
        descripcion = driver.find_element(By.ID, "lblDescripcionDetallePublicacion").text

        # Extraer otros detalles de la sección de "Información"
        operacion = driver.find_element(By.ID, "lblOperacionDetallePublicacion").text
        ciudad = driver.find_element(By.ID, "lblCiudadDetallePublicacion").text
        categoria = driver.find_element(By.ID, "lblCategoriaDetallePublicacion").text
        fecha_publicacion = driver.find_element(By.ID, "lblFechaPublicacionDetallePublicacion").text
        fecha_cierre = driver.find_element(By.ID, "lblFechaCierreDetallePublicacion").text

        # Imprimir para ver los detalles extraídos
        print(f"Detalles extraídos de la licitación: {titulo}, {descripcion}, {operacion}, {ciudad}, {categoria}, {fecha_publicacion}, {fecha_cierre}, Link: {url_licitacion}")

        return {
            "Titulo": titulo,
            "Descripcion": descripcion,
            "Operacion": operacion,
            "Ciudad": ciudad,
            "Categoria": categoria,
            "FechaPublicacion": fecha_publicacion,
            "FechaCierre": fecha_cierre,
            "Link": url_licitacion  # Guardar el enlace de la licitación
        }
    except Exception as e:
        print(f"Error al extraer detalles de la licitación: {e}")
        return None



def navegar_licitaciones(driver):
    licitaciones_visitadas = set()
    licitaciones_detalles = []

    while True:
        driver.refresh()
        time.sleep(3)
        omitir_finalizados(driver)
        time.sleep(2)

        licitaciones_links = obtener_licitaciones_disponibles(driver, licitaciones_visitadas)
        
        if not licitaciones_links:
            print("No hay más licitaciones nuevas por visitar.")
            break

        for licitacion_id, link in licitaciones_links:
            licitaciones_visitadas.add(licitacion_id)
            try:
                # Refrescar el elemento para evitar el StaleElementReferenceException
                link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, f"a[name='{licitacion_id}']"))
                )
                driver.execute_script("arguments[0].click();", link)
                time.sleep(2)

                detalles = extraer_detalles_licitacion(driver)
                if detalles:
                    licitaciones_detalles.append(detalles)

                btn_volver = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnVolverDetallePublicacion"))
                )
                btn_volver.click()
                time.sleep(2)
            except Exception as e:
                print(f"Error al navegar a la licitación '{licitacion_id}': {e}")
                break

    return pd.DataFrame(licitaciones_detalles)

def login_and_scrape():
    driver = setup_driver()
    try:
        driver.get("https://www.sistemasicep.cl/")
        time.sleep(2)
        username_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "nombreUsuario")))
        username_field.send_keys(USER)
        password_field = driver.find_element(By.ID, "passUsuario")
        password_field.send_keys(PASSWORD)
        password_field.send_keys(Keys.RETURN)
        time.sleep(5)

        acceder_nuevas_licitaciones(driver)
        omitir_finalizados(driver)

        df_licitaciones = navegar_licitaciones(driver)
        return df_licitaciones

    finally:
        driver.quit()

if __name__ == "__main__":
    df = login_and_scrape()
    print(df)
