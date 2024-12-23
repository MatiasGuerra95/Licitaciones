from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from datetime import datetime
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    """Configura y devuelve una instancia del controlador de Selenium."""
    options = Options()
    # options.add_argument("--headless")  # Descomenta esta línea si quieres ocultar el navegador
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    service = Service(executable_path="/usr/local/bin/chromedriver")  # Ajusta la ruta si es necesario
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def buscar_licitaciones_enami():
    driver = setup_driver()
    driver.get("https://www.ryce.cl/home/licitaciones.aspx")

    try:
        # Esperar a que el botón sea clickeable
        boton_licitaciones_vigentes = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ver_vigentes"))
        )
        
        # Desplazar la vista para asegurarse de que el botón sea visible
        driver.execute_script("arguments[0].scrollIntoView(true);", boton_licitaciones_vigentes)
        time.sleep(1)  # Breve pausa para asegurar visibilidad
        
        # Hacer clic en el botón usando ActionChains
        ActionChains(driver).move_to_element(boton_licitaciones_vigentes).click().perform()
        logging.info("Botón 'Ver Licitaciones Vigentes' clickeado.")

        # Esperar a que la tabla esté visible
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.TAG_NAME, "table"))
        )
        logging.info("Tabla de licitaciones visible.")

        # Extraer las filas de la tabla
        licitaciones = []
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            # Asegurarse de que la fila tenga al menos 6 columnas
            if len(columns) >= 6:
                termino_inscripciones = columns[5].text.strip()  # Sexta columna: Término Inscripciones
                try:
                    # Convertir la fecha de término a un objeto datetime
                    fecha_termino = datetime.strptime(termino_inscripciones, "%d/%m/%Y")
                    
                    # Verificar que el año sea el actual (2024)
                    if fecha_termino.year == datetime.now().year:
                        licitacion = {
                            "Número": columns[0].text.strip(),
                            "Principal": columns[1].text.strip(),
                            "Licitación": columns[2].text.strip(),
                            "Código": columns[3].text.strip(),
                            "Inicio": columns[4].text.strip(),
                            "Término Inscripciones": termino_inscripciones,
                        }
                        licitaciones.append(licitacion)
                except ValueError:
                    logging.warning(f"Fecha inválida en 'Término Inscripciones': {termino_inscripciones}")
                    continue

        # Convertir los datos en un DataFrame de pandas
        df_licitaciones = pd.DataFrame(licitaciones)
        if not df_licitaciones.empty:
            logging.info(f"Licitaciones extraídas: {len(df_licitaciones)}")
            print(df_licitaciones)
        else:
            logging.warning("No se encontraron licitaciones vigentes.")

    except Exception as e:
        logging.error(f"Error al interactuar con la página de licitaciones de ENAMI: {e}")
    finally:
        time.sleep(3)  # Breve pausa antes de cerrar el navegador
        driver.quit()

if __name__ == "__main__":
    buscar_licitaciones_enami()





