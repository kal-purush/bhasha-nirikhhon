from flask import Flask, request, jsonify, render_template, send_file
import io
from fpdf import FPDF
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    user_details = {
        "name": data.get("name"),
        "email": data.get("email"),
        "phone": data.get("phone"),
    }
    responses = {k: v for k, v in data.items() if k.startswith("q")}
    
    # Risk calculation logic remains the same...
    risk_score = 0
    # Add to the risk_score based on responses as before...

    # Determine risk level
    if risk_score <= 3:
        risk_level = {"level": "Low", "color": "green"}
    elif 4 <= risk_score <= 6:
        risk_level = {"level": "Moderate", "color": "yellow"}
    else:
        risk_level = {"level": "High", "color": "red"}

    user_details["risk_level"] = risk_level["level"]
    
    # Debugging: Log user details
    app.logger.info(f"User Details: {user_details}")
    
    app.user_details = user_details

    return jsonify(risk_level)

@app.route('/download', methods=['GET'])
def download():
    user_details = app.user_details

    # Generate downloadable result
    output = io.StringIO()
    output.write("Blood Cancer Risk Assessment Results\n\n")
    output.write(f"Name: {user_details['name']}\n")
    output.write(f"Email: {user_details['email']}\n")
    output.write(f"Phone: {user_details['phone']}\n")
    output.write(f"Risk Level: {user_details['risk_level']}\n")

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name="risk_assessment_results.txt",
        mimetype="text/plain"
    )


    

@app.route('/download-pdf', methods=['GET'])
def download_pdf():
    user_details = getattr(app, 'user_details', None)

    if not user_details:
        app.logger.error("No user details available for PDF generation.")
        return "No data available to generate PDF. Please complete the assessment first.", 400

    app.logger.info(f"Generating PDF for: {user_details}")

    pdf = FPDF()
    pdf.add_page()

    # Add company logo
    logo_path = "/Users/sawarn/Developer/AI:ML /Chat Bot test/logo.png"  # Replace with the actual path to your logo
    pdf.image(logo_path, x=10, y=8, w=30)  # Adjust the size and position as needed
    pdf.set_font("Arial", style='B', size=16)
    pdf.cell(0, 10, txt="Blood Cancer Risk Assessment Results", ln=True, align='C')
    pdf.ln(20)

    pdf.set_font("Arial", size=12)

    # Add user details
    pdf.cell(0, 10, txt=f"Name: {user_details.get('name', 'N/A')}", ln=True)
    pdf.cell(0, 10, txt=f"Email: {user_details.get('email', 'N/A')}", ln=True)
    pdf.cell(0, 10, txt=f"Phone: {user_details.get('phone', 'N/A')}", ln=True)

    # Add risk level with color
    risk_level = user_details.get('risk_level', 'N/A')
    color_map = {"Low": (0, 255, 0), "Moderate": (255, 255, 0), "High": (255, 0, 0)}
    color = color_map.get(risk_level, (0, 0, 0))  # Default to black if unknown
    pdf.set_text_color(*color)
    pdf.cell(0, 10, txt=f"Risk Level: {risk_level}", ln=True)
    pdf.set_text_color(0, 0, 0)  # Reset to black

    # Footer or additional notes
    pdf.ln(10)
    pdf.cell(0, 10, txt="This assessment is not a definitive diagnosis. Please consult a healthcare professional.", ln=True, align='C')

    # Save and send the PDF
    pdf_output = io.BytesIO()
    pdf_data = pdf.output(dest='S').encode('latin1')
    pdf_output.write(pdf_data)
    pdf_output.seek(0)

    app.logger.info(f"PDF Size: {pdf_output.getbuffer().nbytes} bytes")

    return send_file(
        pdf_output,
        as_attachment=True,
        download_name="risk_assessment_results.pdf",
        mimetype="application/pdf"
    )








if __name__ == "__main__":
    app.run(debug=True)