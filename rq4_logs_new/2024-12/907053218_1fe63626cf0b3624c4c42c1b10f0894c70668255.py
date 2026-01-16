from telegram import Update
from telegram.ext import ContextTypes
from db import save_results, get_ranking  # Importar funciones de DB
from utils import validate_name  # Importar la función de validación


# Función que maneja el comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.message.from_user.first_name
    await update.message.reply_text(f"¡Hola {user_name}! Envía tu nombre y los tiempos de los juegos.")


# Función para manejar el comando /record
async def record_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.message.from_user.first_name
    # Verificar que el nombre sea válido
    if validate_name(user_name):
        # Extraer los tiempos
        times = context.args
        if len(times) == 2:
            # Guardar los resultados en la base de datos
            save_results(user_name, int(times[0]), int(times[1]))
            await update.message.reply_text(f"Resultados de {user_name} guardados correctamente.")
        else:
            await update.message.reply_text("Por favor, envía los tiempos de los dos juegos.")
    else:
        await update.message.reply_text("No estás autorizado para registrar resultados.")


# Función para manejar el comando /results
async def show_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Obtener el ranking de la base de datos
    ranking = get_ranking()
    if ranking:
        response = "Ranking de resultados:\n"
        for rank in ranking:
            response += f"{rank[0]} - Queens: {rank[1]}, Tango: {rank[2]}\n"
    else:
        response = "No hay resultados registrados aún."

    await update.message.reply_text(response)