from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from datetime import datetime

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
        # Hacer clic en el botón "Ver Licitaciones Vigentes"
        boton_licitaciones_vigentes = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ver_vigentes"))
        )
        boton_licitaciones_vigentes.click()
        
        # Esperar a que la tabla esté visible
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.TAG_NAME, "table"))
        )

        # Extraer las filas de la tabla
        licitaciones = []
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            
            # Asegurarse de que la fila tenga todas las columnas esperadas
            if len(columns) >= 5:
                inicio = columns[3].text.strip()  # Extraemos la fecha de "Inicio" (por si necesitamos verificar)
                termino_inscripciones = columns[4].text.strip()  # Extraemos el texto de "Término Inscripciones"
                
                # Mostrar en consola para verificar el contenido extraído
                print(f"Inicio: {inicio}, TÉRMINO INSCRIPCIONES: {termino_inscripciones}")
                
                # Verificar si "Término Inscripciones" está relleno y en el año actual (2024)
                if termino_inscripciones:
                    try:
                        # Intentar convertir "Término Inscripciones" a formato de fecha
                        fecha_termino = datetime.strptime(termino_inscripciones, "%d/%m/%Y")
                        
                        # Verificar que el año de "Término Inscripciones" sea el actual (2024)
                        if fecha_termino.year == datetime.now().year:
                            licitacion = {
                                "Número": columns[0].text.strip(),
                                "Principal": columns[1].text.strip(),
                                "Licitación": columns[2].text.strip(),
                                "Código": columns[3].text.strip(),
                                "Inicio": inicio,
                                "Término Inscripciones": termino_inscripciones
                            }
                            licitaciones.append(licitacion)
                    except ValueError:
                        # Si "Término Inscripciones" no tiene un formato de fecha válido, se ignora
                        continue
        
        # Convertir los datos en un DataFrame de pandas para guardarlos o procesarlos
        df_licitaciones = pd.DataFrame(licitaciones)
        print(df_licitaciones)  # Puedes guardar en CSV o integrarlo en tu sistema

    except Exception as e:
        print(f"Error al interactuar con la página de licitaciones de ENAMI: {e}")
    finally:
        time.sleep(10)  # Pausa para observar el contenido en el navegador antes de cerrar
        driver.quit()

if __name__ == "__main__":
    buscar_licitaciones_enami()



