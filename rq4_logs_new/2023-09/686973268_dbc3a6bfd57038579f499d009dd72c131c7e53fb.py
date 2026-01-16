import qrcode

def generate_qr_code(data, filename):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="#043671", back_color="#1ac6ff")
    img.save(filename)

if __name__ == "__main__":
    landing_page_url = "https://sammalishe.github.io/QRGen4b/"

    generate_qr_code(landing_page_url, "landing_page_qrcode.png")
    print(f"Landing Page QR Code generated and saved as landing_page_qrcode.png")