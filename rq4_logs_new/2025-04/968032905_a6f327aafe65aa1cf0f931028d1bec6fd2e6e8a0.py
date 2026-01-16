from flask import Flask, render_template, request, send_file, jsonify, make_response
import os
import tempfile
import pyzipper
import io
import webbrowser
import threading
import time
import shutil
from werkzeug.utils import secure_filename
from pdf2pdfa import convert_to_pdfa

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Limite de 16MB

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/convert', methods=['POST'])
def convert():
    if 'pdf_file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Nenhum arquivo enviado'}), 400
    
    pdf_file = request.files['pdf_file']
    if pdf_file.filename == '':
        return jsonify({'status': 'error', 'message': 'Nenhum arquivo selecionado'}), 400

    password = request.form.get('password', '')
    
    if not pdf_file.filename.lower().endswith('.pdf'):
        return jsonify({'status': 'error', 'message': 'O arquivo deve ser um PDF'}), 400

    try:
        original_name = os.path.splitext(secure_filename(pdf_file.filename))[0]
        temp_dir = tempfile.mkdtemp()
        
        # Processamento do PDF
        temp_input_path = os.path.join(temp_dir, "original.pdf")
        pdf_file.save(temp_input_path)
        temp_pdfa_path = os.path.join(temp_dir, f"{original_name}.pdf")
        convert_to_pdfa(temp_input_path, temp_pdfa_path)
        
        # Criação do ZIP
        zip_buffer = io.BytesIO()
        
        with pyzipper.AESZipFile(zip_buffer, 'w', compression=pyzipper.ZIP_LZMA) as zipf:
            with open(temp_pdfa_path, 'rb') as f:
                pdf_data = f.read()
            
            if password:
                zipf.setpassword(password.encode('utf-8'))
                zipf.setencryption(pyzipper.WZ_AES, nbits=256)
            
            zipf.writestr(f"{original_name}.pdf", pdf_data)
        
        zip_buffer.seek(0)
        
        response = make_response(send_file(
            zip_buffer,
            as_attachment=True,
            download_name=f"{original_name}.zip",
            mimetype='application/zip'
        ))
        
        # Adiciona cookie para sinalizar conclusão
        response.set_cookie('downloadComplete', 'true', max_age=10)
        return response
        
    except Exception as e:
        app.logger.error(f"Erro durante processamento: {str(e)}")
        return jsonify({'status': 'error', 'message': f'Erro durante a conversão: {str(e)}'}), 500
    finally:
        try:
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

def open_browser():
    time.sleep(1.5)
    webbrowser.open('http://localhost:5000')

if __name__ == '__main__':
    threading.Thread(target=open_browser).start()
    app.run(debug=False, port=5000)