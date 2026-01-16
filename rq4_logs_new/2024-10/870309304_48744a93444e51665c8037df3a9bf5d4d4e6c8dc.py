from faker import Faker
import random
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import os

ascii_art = """Bonny Airdrop Bot V1.0.0
Join here: https://demo.bonny.so/login
Use this Code to Activate the Bot: "olaf"
⠀⠀⠀⠀⠀⢀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢰⣿⡿⠗⠀⠠⠄⡀⠀⠀⠀⠀
⠀⠀⠀⠀⡜⠁⠀⠀⠀⠀⠀⠈⠑⢶⣶⡄
⢀⣶⣦⣸⠀⢼⣟⡇⠀⠀⢀⣀⠀⠘⡿⠃
⠀⢿⣿⣿⣄⠒⠀⠠⢶⡂⢫⣿⢇⢀⠃⠀
⠀⠈⠻⣿⣿⣿⣶⣤⣀⣀⣀⣂⡠⠊⠀⠀
⠀⠀⠀⠃⠀⠀⠉⠙⠛⠿⣿⣿⣧⠀⠀⠀
⠀⠀⠘⡀⠀⠀⠀⠀⠀⠀⠘⣿⣿⡇⠀⠀
⠀⠀⠀⣷⣄⡀⠀⠀⠀⢀⣴⡟⠿⠃⠀⠀
⠀⠀⠀⢻⣿⣿⠉⠉⢹⣿⣿⠁⠀⠀⠀⠀
⠀⠀⠀⠀⠉⠁⠀⠀⠀⠉⠁⠀⠀⠀⠀⠀
"""

a = Faker()

def b():
    c = a.company()
    d = a.address().split('\n')[0]
    e = datetime.now().strftime('%m/%d/%Y    %I:%M %p')
    f = a.uuid4()[:8].upper()
    g = 'MCC - ' + a.bothify(text='??#####')
    h = random.choice(['VISA', 'MASTERCARD', 'AMEX'])
    i = a.credit_card_number(card_type=h.lower())
    j = i[-4:]
    k = round(random.uniform(20.00, 100.00), 2)
    l = round(k * 0.01, 2)
    m = round(k + l, 2)

    return {
        "z": c,
        "x": d,
        "y": e,
        "w": f,
        "v": g,
        "u": h,
        "t": j,
        "s": k,
        "r": l,
        "q": m
    }

def n(o, p, q):
    r = Image.new('RGB', (400, 400), color=(255, 255, 255))
    s = ImageDraw.Draw(r)
    t = ImageFont.load_default()
    u = [
        o["z"],
        o["x"],
        "",
        "--------------------------------------",
        o["y"],
        "--------------------------------------",
        f"TRANS - {o['w']}",
        o["v"],
        f"PAYMENT - {o['u']} **** {o['t']}",
        "--------------------------------------",
        f"SUBTOTAL: ${o['s']}",
        f"TAX: ${o['r']}",
        f"TOTAL: ${o['q']}",
        "",
        "PLEASE COME AGAIN",
        "THANK YOU"
    ]
    
    v = 20
    for w in u:
        s.text((20, v), w, font=t, fill=(0, 0, 0))
        v += 20

    x = f"{q}/receipt_{p}.png"
    r.save(x)

def y(z, a1):
    for a2 in range(z):
        a3 = b()
        n(a3, a2+1, a1)
        print(f"Generated receipt {a2+1}")

def main():
    print(ascii_art)
    
    b1 = int(input("How many receipts would you like to generate? "))

    c1 = os.path.join(os.getcwd(), "Generated Receipts")
    os.makedirs(c1, exist_ok=True)

    y(b1, c1)

    print(f"All {b1} receipts have been generated and saved in '{c1}'.")
    input("Press Enter to exit.")

if __name__ == "__main__":
    main()