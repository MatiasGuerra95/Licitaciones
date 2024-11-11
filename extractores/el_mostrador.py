from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import time

def setup_driver():
    """Configures the Selenium WebDriver with explicit path."""
    options = Options()
    # options.add_argument("--headless")  # Puedes comentar esta línea si quieres ver el navegador
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def buscar_licitaciones_el_mostrador():
    driver = setup_driver()
    driver.get("https://legales.elmostrador.cl/")

    try:
        # Esperar a que el campo de palabras clave esté presente y escribir 'Licitación'
        palabra_clave_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "keywords"))
        )
        palabra_clave_input.clear()
        palabra_clave_input.send_keys("Licitación")

        # Seleccionar la categoría 'Licitaciones, Ventas, Remates y Propuestas' en el dropdown
        categoria_select = Select(driver.find_element(By.ID, "select-category"))
        categoria_select.select_by_visible_text("Licitaciones, Ventas, Remates y Propuestas")

        # Hacer clic en el botón de "Buscar"
        buscar_button = driver.find_element(By.CSS_SELECTOR, "div.a-field.submit input[type='submit']")
        buscar_button.click()
        
        print("Búsqueda realizada con éxito.")

        # Aquí puedes agregar lógica para extraer los resultados, si es necesario
        
    except Exception as e:
        print(f"Error al realizar la búsqueda: {e}")
    finally:
        time.sleep(30)  # Espera para ver el resultado antes de cerrar el navegador
        driver.quit()

if __name__ == "__main__":
    buscar_licitaciones_el_mostrador()

