from flask import Flask, request, jsonify
from PIL import Image
# import pytesseract
import google.generativeai as genai
import os

app = Flask(__name__)

# Set up your Google Generative AI model
model = genai.GenerativeModel(model_name='gemini-1.5-pro-latest')

@app.route('/extract_text', methods=['POST'])
def extract_text():
    if 'image' not in request.files:
        return jsonify({"error": "No image file provided"}), 400
    
    image_file = request.files['image']
    
    # Open the image using PIL
    img = Image.open(image_file)
    
    # Use Google Generative AI to extract text
    prompt = "Give me the text in the image in json format"
    response = model.generate_content([prompt, img])
    
    # Return the extracted text as JSON
    return jsonify({"extracted_text": response.text})

if __name__ == '__main__':
    app.run(debug=True)