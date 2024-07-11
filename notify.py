import pymysql
import requests
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

# Obtener variables de entorno
webhook_url = os.getenv("GOOGLE_CHAT_WEBHOOK_URL")
db_config = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME")
}

# Función para notificar incidencias (adaptada a incidencias_carretera)
def notificar_incidencia(incidencia):
    payload = {
        'text': f"Nueva incidencia en carretera:\n"
                f"Carretera: {incidencia['carretera']}\n"
                f"Municipio: {incidencia['municipio']}\n"
                f"Sentido: {incidencia['direccion']}\n"
                f"KM: {incidencia['km']}\n"
                f"Causa: {incidencia['causa']}\n"
                f"Descripción: {incidencia['descripcion']}\n"
                f"Fecha y hora: {incidencia['fecha']} {incidencia['hora']}"
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error al enviar la notificación: {e}")
        return False

try:
    # Conexión a la base de datos
    connection = pymysql.connect(**db_config)

    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        # Consulta para obtener la incidencia de carretera más antigua no notificada
        sql = """
            SELECT carretera, municipio, direccion, km, causa, descripcion, fecha, hora 
            FROM incidencias_carretera
            WHERE notificado = 'no' 
            ORDER BY fecha ASC, hora ASC 
            LIMIT 1
        """
        cursor.execute(sql)
        incidencia = cursor.fetchone()

        if incidencia:
            # Notificar la incidencia
            exito = notificar_incidencia(incidencia)

            if exito:
                # Actualizar el estado de la incidencia a notificada (incluyendo todas las columnas en el WHERE)
                update_sql = """
                    UPDATE incidencias_carretera 
                    SET notificado = 'si' 
                    WHERE carretera = %s AND municipio = %s AND direccion = %s AND km = %s 
                          AND causa = %s AND descripcion = %s AND fecha = %s AND hora = %s
                """
                cursor.execute(update_sql, (incidencia['carretera'], incidencia['municipio'], 
                                           incidencia['direccion'], incidencia['km'], 
                                           incidencia['causa'], incidencia['descripcion'], 
                                           incidencia['fecha'], incidencia['hora']))
                connection.commit()

except pymysql.MySQLError as e:
    if e.args[0] == 2003:
        print(f"Error de conexión a MySQL: No se puede conectar al servidor. Verifica la configuración.")
    elif e.args[0] == 1045:
        print(f"Error de autenticación en MySQL: Usuario o contraseña incorrectos.")
    else:
        print(f"Error en la base de datos: {e}")

finally:
    # Cerrar la conexión
    if connection:
        connection.close()
