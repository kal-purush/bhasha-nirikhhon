import qrcode

# Function to generate a QR code that links to a specific URL
def generate_qr_code(qr_code_id, base_url):
    url = f"{base_url}/scan/{qr_code_id}"  # URL of your Flask route or website
    qr = qrcode.make(url)  # Generate the QR code
    qr.save(f"{qr_code_id}.png")  # Save the QR code as a PNG file

# List of QR codes to generate (for different exhibits)
exhibit_ids = [
    {"id": "fish001", "name": "Fish Exhibit"},
    {"id": "monkey001", "name": "Monkey Exhibit"},
    {"id": "tank001", "name": "Tank Exhibit"}
]

# Your base URL (update it if you deploy the app or use a different domain)
base_url = "gioinfocus.com"  # Replace with your actual domain

# Generate QR codes for each exhibit
for exhibit in exhibit_ids:
    generate_qr_code(exhibit["id"], base_url)
    print(f"QR code generated for {exhibit['name']} with ID {exhibit['id']}")