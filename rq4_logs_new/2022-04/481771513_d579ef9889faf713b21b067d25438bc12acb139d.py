from neopixel import NeoPixel
from time import sleep
import pandas as pd

strand = NeoPixel('/dev/ttyACM0', baudrate=115200)

width = 16
height = 16
def main(fname):
    # clear panel
    for x in range(width*height):
        print(x)
        strand.setPixelColor(x, 0, 0, 0)
        #sleep(1)
    strand.show()

    df = pd.read_csv(fname)
    print(df)
    for _, row in df.iterrows():
        d = {"index": int(row["index"])}
        try:
            for c in ["red", "green", "blue"]:
                d[c] = int(row[c])
        except ValueError:
            d["red"] = int(255*3/4)
            d["green"] = int(180*3/4)
            d["blue"] = 0

        print(d)
        strand.setPixelColor(d["index"], d["red"], d["green"], d["blue"])
    strand.show()


if __name__ == "__main__":
    main("right.txt")