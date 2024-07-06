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
google_chat_webhook_url = os.getenv('GOOGLE_CHAT_WEBHOOK_URL')
API_URL = os.getenv("API_URL")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Lista de URLs de feeds RSS de carreteras
rss_urls = {
    'Transporte Metropolitano': 'https://gencat.cat/transit/opendata/001_RSS.xml',
    # Agrega aquí más URLs de feeds RSS de carreteras según tus necesidades
}

# Conexión a MySQL usando PyMySQL (GLOBAL)
cnx = None

proxys_cache = {}


def obtener_proxies():
    if proxys_cache.get("proxys"):
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


def notificar_incidencia_carretera(webhook_url, incidencia, nombre_carretera):
    payload = {
        'text': f"Incidencia en {incidencia['carretera']} ({incidencia['municipio']}):\n"
                f"Tipo: {incidencia['descripcion']}\n"  # Usamos 'descripcion' aquí
                f"Causa: {incidencia['causa']}\n"
                f"Dirección: {incidencia['direccion']}\n"
                f"KM: {incidencia['km']}"
    }
    requests.post(webhook_url, json=payload)


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
        carreteras_con_incidencias = set()
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
                    notificar_incidencia_carretera(google_chat_webhook_url, incidencia, nombre_carretera)
                    carreteras_con_incidencias.add(nombre_carretera)
                    ultimas_incidencias.append({
                        'descripcion': incidencia['descripcion'],
                        'fecha': datetime.now().strftime('%Y-%m-%d')
                    })

                registrar_incidencia_carretera(cursor, nombre_carretera, incidencia)  # Intenta registrar (puede ser duplicado)

        if carreteras_con_incidencias:
            mensaje_final = f"Resumen de incidencias en las carreteras: {', '.join(carreteras_con_incidencias)}"
            payload = {'text': mensaje_final}
            requests.post(google_chat_webhook_url, json=payload)

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

if __name__ == "__main__":
    main()