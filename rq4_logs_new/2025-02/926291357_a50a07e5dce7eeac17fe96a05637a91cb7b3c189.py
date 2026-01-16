import os
from flask import Flask, request, render_template, send_file
from werkzeug.utils import secure_filename
from aspose.slides import Presentation

import os
app = Flask(__name__, template_folder=os.path.join(os.getcwd(), "app/templates"))


# Configure Upload & Output Folders
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "converted_svgs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER
app.config["ALLOWED_EXTENSIONS"] = {"ppt", "pptx"}

def allowed_file(filename):
    """Check if file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]

@app.route("/", methods=["GET", "POST"])
def upload_file():
    """Handle file uploads and conversion."""
    if request.method == "POST":
        file = request.files["file"]
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(file_path)

            # Convert PPT to SVG
            ppt = Presentation(file_path)
            base_name = os.path.splitext(filename)[0]
            svg_files = []

            for i, slide in enumerate(ppt.slides):
                svg_filename = f"{base_name}_slide_{i+1}.svg"
                svg_path = os.path.join(app.config["OUTPUT_FOLDER"], svg_filename)
                with open(svg_path, "wb") as svg_file:
                    slide.write_as_svg(svg_file)
                svg_files.append(svg_filename)

            return render_template("result.html", svg_files=svg_files, base_name=base_name)

    return render_template("index.html")

@app.route("/converted_svgs/<filename>")
def download_file(filename):
    """Serve converted SVG files as a forced download."""
    file_path = os.path.join(app.config["OUTPUT_FOLDER"], filename)

    # Debugging output
    print(f"Downloading file: {file_path}")

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)  # Force download
    else:
        return "File Not Found", 404

if __name__ == "__main__":
    app.run(debug=True)