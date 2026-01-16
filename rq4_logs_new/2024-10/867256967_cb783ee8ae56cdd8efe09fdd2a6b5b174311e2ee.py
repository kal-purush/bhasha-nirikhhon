from PIL import Image

IDS = ["two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "jack", "queen", "king", "ace"]
SUITS = ["hearts", "clubs", "diamonds", "spades"]

for i in range(4):
    for j in range(13):
        box = (j * 71, i * 95, (j + 1) * 71, (i + 1) * 95)
        sheet = Image.open("balatro.png")
        print(box)
        new = sheet.crop(box)
        new.save(f"src/assets/cards/{IDS[j]}-of-{SUITS[i]}.png")