import google.generativeai as genai
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configurar la API Key de Google Generative AI directamente en el script
api_key = "AIzaSyCO6lrsxiYODxCPDFt1X64vfMWb4Fu_km0"
genai.configure(api_key=api_key)

# Configuración del modelo de generación
generation_config = {
  "temperature": 1,
  "top_p": 0.95,
  "top_k": 64,
  "max_output_tokens": 8192,
  "response_mime_type": "text/plain",
}

# Crear el modelo de generación
model = genai.GenerativeModel(
  model_name="gemini-1.5-pro",
  generation_config=generation_config,
  # Ajuste de configuraciones de seguridad si es necesario
  # safety_settings = ...
)

@app.route('/generate', methods=['POST'])
def generate_incident():
    input_text = request.json.get('input')
    response = model.generate_content([input_text])
    return jsonify({'output': response.text})

if __name__ == '__main__':
    app.run(debug=True)