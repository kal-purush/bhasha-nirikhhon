from flask import Flask, request, jsonify
import os
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/process', methods=['POST'])
def process_message():
    try:
        # Get the text message
        message = request.form.get("message", "")

        # Get the uploaded file
        file = request.files.get("file")
        file_content = ""
        if file:
            file_path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(file_path)  # Save the file
            with open(file_path, "r") as f:
                file_content = f.read()

        # Combine message and file content
        combined_message = f"Message: {message}\nFile Content: {file_content}"

        # Write combined message to a text file
        output_file_path = os.path.join(UPLOAD_FOLDER, "output.txt")
        with open(output_file_path, "w") as output_file:
            output_file.write(combined_message)

        # Return combined message
        return jsonify({"message": combined_message}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)