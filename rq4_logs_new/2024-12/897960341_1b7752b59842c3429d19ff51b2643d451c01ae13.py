from flask import Flask, render_template, request, send_from_directory
from PIL import Image, ImageDraw, ImageFont
import os

app = Flask(__name__)

# Configurations
UPLOAD_FOLDER = "static/uploads"
OUTPUT_FOLDER = "static/generated_cards"
TEMPLATE_PATH = "card_template.png"
FONT_PATH = "arial.ttf"  # Replace with your font file path
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER

# Ensure folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Function to generate the exam card
def generate_exam_card(full_name, matric_no, level, photo_path, output_path):
    template = Image.open(TEMPLATE_PATH)
    draw = ImageDraw.Draw(template)

    # Load fonts
    font_large = ImageFont.truetype(FONT_PATH, 40)
    font_small = ImageFont.truetype(FONT_PATH, 30)

    # Add text to the card
    draw.text((600, 270), full_name, font=font_large, fill="black")
    draw.text((600, 370), matric_no, font=font_small, fill="black")
    draw.text((600, 450), level, font=font_small, fill="black")

    # Add the student's photo
    student_photo = Image.open(photo_path).resize((320, 320))
    template.paste(student_photo, (30, 180))

    # Save the final card
    template.save(output_path)

# Route for the form
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Get form data
        full_name = request.form["full_name"]
        matric_no = request.form["matric_no"]
        level = request.form["level"]

        # Handle file upload
        photo = request.files["photo"]
        photo_path = os.path.join(app.config["UPLOAD_FOLDER"], photo.filename)
        photo.save(photo_path)

        # Generate the card
        output_path = os.path.join(app.config["OUTPUT_FOLDER"], f"{matric_no}_card.png")
        generate_exam_card(full_name, matric_no, level, photo_path, output_path)

        # Provide download link
        return send_from_directory(app.config["OUTPUT_FOLDER"], f"{matric_no}_card.png", as_attachment=True)

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0',port=5000)