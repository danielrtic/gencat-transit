import os
import feedparser
import requests
import random
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pymysql

# Cargar variables desde .env
load_dotenv()
API_URL = os.getenv("API_URL")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Lista de URLs de feeds RSS de carreteras
rss_urls = {
    'AP-7': 'https://gencat.cat/transit/opendata/001_RSS.xml',
    'A-2': 'https://gencat.cat/transit/opendata/005_RSS.xml',
    'AP-2': 'https://gencat.cat/transit/opendata/003_RSS.xml',
    'A-7': 'https://gencat.cat/transit/opendata/024_RSS.xml',
    'B-23': 'https://gencat.cat/transit/opendata/004_RSS.xml',
    'B-20': 'https://gencat.cat/transit/opendata/035_RSS.xml',
    'C-58': 'https://gencat.cat/transit/opendata/010_RSS.xml',
    'C-25': 'http://www.gencat.cat/transit/opendata/015_RSS.xml',
    'C-14': 'http://www.gencat.cat/transit/opendata/026_RSS.xml',
    'C-35': 'http://www.gencat.cat/transit/opendata/026_RSS.xml',
    'C-13': 'http://www.gencat.cat/transit/opendata/030_RSS.xml',
    'C-16': 'http://www.gencat.cat/transit/opendata/011_RSS.xml',
    'C-37': 'http://www.gencat.cat/transit/opendata/018_RSS.xml',
    'C-65': 'http://www.gencat.cat/transit/opendata/020_RSS.xml',
    'C-12': 'http://www.gencat.cat/transit/opendata/027_RSS.xml',
    'C-66': 'http://www.gencat.cat/transit/opendata/022_RSS.xml',
    'C-55': 'http://www.gencat.cat/transit/opendata/017_RSS.xml',
    'C-33': 'http://www.gencat.cat/transit/opendata/022_RSS.xml',
    'C-60': 'http://www.gencat.cat/transit/opendata/016_RSS.xml',
    'C-31 Costa Brava': 'http://www.gencat.cat/transit/opendata/021_RSS.xml',
    'C-32 Sud': 'http://www.gencat.cat/transit/opendata/007_RSS.xml',
    'C-31 Nord': 'http://www.gencat.cat/transit/opendata/008_RSS.xml',
    'C-32 Nord': 'http://www.gencat.cat/transit/opendata/009_RSS.xml',
    'C-31 Sud': 'http://www.gencat.cat/transit/opendata/006_RSS.xml',
    'C-17': 'http://www.gencat.cat/transit/opendata/013_RSS.xml',
    'N-420': 'http://www.gencat.cat/transit/opendata/025_RSS.xml',
    'N-145': 'http://www.gencat.cat/transit/opendata/032_RSS.xml',
    'N-340': 'http://www.gencat.cat/transit/opendata/023_RSS.xml',
    'N-230': 'http://www.gencat.cat/transit/opendata/028_RSS.xml',
    'N-240': 'http://www.gencat.cat/transit/opendata/029_RSS.xml',
    'N-152': 'http://www.gencat.cat/transit/opendata/033_RSS.xml',
    'N-II': 'http://www.gencat.cat/transit/opendata/014_RSS.xml',
    'N-260': 'http://www.gencat.cat/transit/opendata/031_RSS.xml'
    
    
    # Agrega aquí más URLs de feeds RSS de carreteras según tus necesidades
}

 
# Conexión a MySQL usando PyMySQL (GLOBAL)
cnx = None

proxys_cache = {}


def obtener_proxies():
    if proxys_cache.get("proxys") and (datetime.now() - proxys_cache["timestamp"]) < timedelta(minutes=5):
        return proxys_cache["proxys"]

    response = requests.get(API_URL)
    if response.status_code == 200:
        proxys = response.text.splitlines()
        proxys_formateados = []
        for proxy in proxys:
            partes = proxy.split(":")
            if len(partes) == 4:
                ip, puerto, usuario, contraseña = partes
                proxy_formateado = f"{ip}:{puerto}:{usuario}:{contraseña}"
                proxys_formateados.append(proxy_formateado)

        proxys_cache["proxys"] = proxys_formateados
        proxys_cache["timestamp"] = datetime.now()
        return proxys_formateados
    else:
        print("Error al obtener proxies de la API.")
        return []


def usar_proxy_aleatorio(url_carretera):
    proxys = obtener_proxies()
    if not proxys:
        return

    proxy_elegido = random.choice(proxys)
    ip, puerto, usuario, contraseña = proxy_elegido.split(":")
    proxies = {
        "http": f"http://{usuario}:{contraseña}@{ip}:{puerto}",
        "https": f"http://{usuario}:{contraseña}@{ip}:{puerto}",
    }

    try:
        response = requests.get(url_carretera, proxies=proxies, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error al usar el proxy: {e}")
        return None


def obtener_incidencias_carretera(url_carretera):
    use_proxy = os.getenv("USE_PROXY") == "on"
    if use_proxy:
        response = usar_proxy_aleatorio(url_carretera)
    else:
        response = requests.get(url_carretera, timeout=10)
        response.raise_for_status()

    if response is None:
        return []

    feed = feedparser.parse(response.content)

    incidencias = []
    for entry in feed.entries:
        description = entry.summary.strip()

        carretera = re.search(r"^[A-Z]{1,2}-?\d+", description).group(0)
        municipio = re.search(r"\s*\|\s*(.*?)\s*\|\s*", description).group(1)
        direccion = re.search(r"Sentit (.*?) \|", description).group(1)
        km_match = re.search(r"Punt km\. ([\d\.]+-[\d\.]+)", description)
        km = km_match.group(1) if km_match else None
        causa = entry.title.split(".")[0].strip()
        tipo_incidencia = entry.title.split(".")[1].strip()
        fecha_hora = datetime.now()  # Fecha y hora actual

        incidencias.append({
            'carretera': carretera,
            'municipio': municipio,
            'direccion': direccion,
            'km': km,
            'causa': causa,
            'descripcion': tipo_incidencia,  # Usamos 'descripcion' aquí
            'fecha_hora': fecha_hora
        })

    return incidencias


def registrar_incidencia_carretera(cursor, nombre_carretera, incidencia):
    fecha = incidencia['fecha_hora'].strftime('%Y-%m-%d')
    hora = incidencia['fecha_hora'].strftime('%H:%M:%S')

    # Verificar si la incidencia ya existe en el mismo día
    cursor.execute(
        "SELECT * FROM incidencias_carretera WHERE carretera = %s AND descripcion = %s AND fecha = %s",
        (incidencia['carretera'], incidencia['descripcion'], fecha)  # Usamos 'descripcion' aquí
    )
    if not cursor.fetchone():
        try:
            cursor.execute(
                "INSERT INTO incidencias_carretera (carretera, municipio, direccion, km, causa, descripcion, fecha, hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (incidencia['carretera'], incidencia['municipio'], incidencia['direccion'], incidencia['km'], incidencia['causa'], incidencia['descripcion'], fecha, hora)
            )
        except pymysql.Error as e:
            print(f"Error al insertar incidencia en MySQL: {e}")
        else:
            cnx.commit()



def cargar_ultimas_incidencias_carretera(cursor):
    try:
        cursor.execute("SELECT descripcion, fecha FROM incidencias_carretera ORDER BY fecha DESC, hora DESC")  # Usamos 'descripcion' aquí
        return [{'descripcion': row[0], 'fecha': row[1].strftime('%Y-%m-%d')} for row in cursor.fetchall()]
    except pymysql.Error as e:
        print(f"Error al cargar últimas incidencias desde MySQL: {e}")
        return []  # Devolver lista vacía en caso de error


def main():
    global cnx  # Indicar que estamos usando la variable global cnx
    try:
        # Conexión a MySQL usando PyMySQL
        cnx = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print("Conexión a MySQL exitosa")  # Confirmar conexión
        cursor = cnx.cursor()

        ultimas_incidencias = cargar_ultimas_incidencias_carretera(cursor)
        carreteras_con_incidencias = set()  # Para almacenar las carreteras con nuevas incidencias

        for nombre_carretera, url_carretera in rss_urls.items():
            incidencias = obtener_incidencias_carretera(url_carretera)
            for incidencia in incidencias:
                incidencia_notificada = False
                for ultima_incidencia in ultimas_incidencias:
                    if (ultima_incidencia['descripcion'] == incidencia['descripcion'] and
                            ultima_incidencia['fecha'] == datetime.now().strftime('%Y-%m-%d')):
                        incidencia_notificada = True
                        break

                if not incidencia_notificada:
                    carreteras_con_incidencias.add(nombre_carretera)  # Marcar la carretera con nueva incidencia
                    ultimas_incidencias.append({  # Actualizar la lista de últimas incidencias
                        'descripcion': incidencia['descripcion'],
                        'fecha': datetime.now().strftime('%Y-%m-%d')
                    })

                registrar_incidencia_carretera(cursor, nombre_carretera, incidencia)

        # Opcional: Aquí podrías agregar código para generar un informe o realizar alguna acción con las carreteras que tuvieron nuevas incidencias

    except pymysql.MySQLError as e:  # Capturar errores específicos de MySQL
        if e.args[0] == 2003:
            print(f"Error de conexión: No se puede conectar al servidor MySQL. Verifica el host y el puerto.")
        elif e.args[0] == 1045:
            print(f"Error de acceso: Usuario o contraseña incorrectos.")
        else:
            print(f"Error general de MySQL: {e}")
    finally:
        if cnx:  # Verificar si la conexión se estableció antes de cerrarla
            cursor.close()
            cnx.close()
