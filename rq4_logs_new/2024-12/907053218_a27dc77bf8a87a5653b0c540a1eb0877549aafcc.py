import sqlite3
from datetime import datetime
import pendulum

DB_NAME = "results.db"


def init_db():
    # Inicializar la base de datos si no existe
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS resultados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT,
                    juego_queens INTEGER,
                    juego_tango INTEGER,
                    fecha_hora TEXT)''')
    conn.commit()
    conn.close()


def save_results(nombre, juego_queens, juego_tango):
    # Obtener la fecha y hora actual
    fecha_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Guardar los resultados en la base de datos
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''INSERT INTO resultados (nombre, juego_queens, juego_tango, fecha_hora)
                 VALUES (?, ?, ?, ?)''', (nombre, juego_queens, juego_tango, fecha_hora))
    conn.commit()
    conn.close()


def get_ranking():
    # Obtener la hora actual en la zona horaria de España
    spain_tz = pendulum.timezone('Europe/Madrid')
    now = pendulum.now(spain_tz)
    today = now.date()

    # Si la hora actual es antes de las 9:00 AM, obtenemos las 9:00 AM de ayer
    if now.hour < 9:
        # 9:00 AM de ayer
        nine_am_yesterday = pendulum.datetime(today.year, today.month, today.day, 9, 0, 0, tz=spain_tz).subtract(days=1)
        nine_am_str = nine_am_yesterday.to_datetime_string()
    else:
        # 9:00 AM de hoy
        nine_am_today = pendulum.datetime(today.year, today.month, today.day, 9, 0, 0, tz=spain_tz)
        nine_am_str = nine_am_today.to_datetime_string()

    # Conectar a la base de datos y filtrar los resultados
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT nombre, juego_queens, juego_tango, fecha_hora FROM resultados WHERE fecha_hora >= ? ORDER BY fecha_hora ASC", (nine_am_str,))
    ranking = c.fetchall()
    conn.close()
    return ranking