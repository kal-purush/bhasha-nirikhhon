from flask import Flask, render_template, request, send_from_directory
from gtts import gTTS
import os
import random

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'static/audio'

class Chatbot:
    def __init__(self):
        self.greetings = ["Hola, escribe tu pregunta:➥"]

    def greet(self):
        return random.choice(self.greetings)

    def speak(self, text):
        tts = gTTS(text=text, lang='es')
        filename = "response.mp3"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        tts.save(filepath)
        return filepath

    def tipos_de_investigacion(self):
        return (
            "Los tipos de investigación son los siguientes:\n"
            "1. Investigación Etnográfica: Este tipo de investigación se basa en la observación de un grupo o cultura y se utiliza comúnmente en el campo de la antropología.\n"
            "2. Investigación Cualitativa: Este tipo de investigación se centra en entender los pensamientos, experiencias y comportamientos de las personas a través de métodos como entrevistas y observaciones.\n"
            "3. Investigación Evaluativa: Este tipo de investigación se utiliza para evaluar la efectividad de un programa, política o intervención.\n"
            "4. Investigación de Mercado: Este tipo de investigación se utiliza para recopilar información sobre los consumidores y el mercado para informar las decisiones de negocio.\n"
            "5. Investigación Comparada: Este tipo de investigación se utiliza para comparar dos o más grupos o casos en relación con una o más variables.\n"
            "6. Investigación Participativa: Este tipo de investigación implica a los participantes en el proceso de investigación, a menudo con el objetivo de empoderar a la comunidad o mejorar las condiciones.\n"
            "7. Expost Facto: Este tipo de investigación se utiliza para estudiar las causas y efectos de los hechos después de que han ocurrido.\n"
            "8. Estudio de Caso: Este tipo de investigación se centra en un caso específico dentro de su contexto real.\n"
            "9. Investigación Correlacional: Este tipo de investigación se utiliza para estudiar la relación entre dos o más variables.\n"
            "10. Investigación Histórica: Este tipo de investigación se utiliza para estudiar eventos, ideas, instituciones, culturas y personas del pasado."
        )

    def tipos_de_patentes(self):
        return (
            "Los tipos de enfoques son:\n"
            "1. Enfoque cuantitativo: La investigación cuantitativa recoge datos para probar ideas en estudios científicos. Se usa información numérica y estadística para probar hipótesis sobre un tema, asegurando que el conocimiento sea objetivo.\n"
            "2. Enfoque cualitativo: Es un método que usa la recolección y análisis de datos para afinar preguntas de investigación o revelar nuevas interrogantes, no recurre a datos de medición numérica. En su lugar utiliza descripciones profundas e interpretaciones de fenómenos como entrevistas o análisis de contenido textual.\n"
            "3. Enfoque mixto: La investigación mixta es una metodología de investigación que consiste en recopilar, analizar e integrar tanto investigación cuantitativa como cualitativa. Este enfoque se utiliza cuando se requiere una mejor comprensión del problema de investigación, y que no te podría dar cada uno de estos métodos por separado.\n"
        )

    def respond(self, message):
        if "cuales son los tipos de investigacion" in message.lower():
            response = self.tipos_de_investigacion()
        elif "cuales son los tipos de enfoques" in message.lower():
            response = self.tipos_de_patentes()
        else:
            response = "Lo siento, no entiendo tu solicitud. ¿Podrías ser más específico?"
        
        audio_path = self.speak(response)
        return response, audio_path

chatbot = Chatbot()

@app.route('/')
def index():
    greeting = chatbot.greet()
    audio_path = chatbot.speak(greeting)
    return render_template('index.html', greeting=greeting, audio_path=audio_path)

@app.route('/ask', methods=['POST'])
def ask():
    message = request.form['message']
    response, audio_path = chatbot.respond(message)
    return render_template('index.html', greeting=chatbot.greet(), response=response, audio_path=audio_path)

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)