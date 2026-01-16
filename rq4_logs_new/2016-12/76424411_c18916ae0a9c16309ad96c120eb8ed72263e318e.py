#!/usr/bin/env python

from __future__ import print_function

class LCD(object):
    # 50 cols x 6 rows
    display = [
            [False]*50,
            [False]*50,
            [False]*50,
            [False]*50,
            [False]*50,
            [False]*50]

    def __init__(self):
        print("Initializing array of %d rows by %d columns" %(len(self.display), len(self.display[0])))

    def get_lit(self):
        count = 0
        for row in self.display:
            count = count + row.count(True)
        return count

    def __str__(self):
        d = ""
        for i in self.display:
            d = d + " ".join(['T' if x else 'F' for x in i]) + "\n"
        return d

    def rect(self, A, B):
        """ Turns on all pixels in a rectangle A wide and B tall starting from upper left """
        print("Turning on rectangle %d by %d" %(A, B))
        for i in range(B):
            self.display[i][:A] = [True]*A
            print(self.display[i][:A])

    def rotate_row(self, A, B):
        """
        shifts all of the pixels in row A (0 is the top row) right by B pixels.
        Pixels that would fall off the right end appear at the left end of the row.
        """
        print("Shifting row %d by %d pixels" %(A, B))
        self.display[A] = self.display[A][len(self.display[A])-B-1:] + self.display[A][:len(self.display[A])-B-1]

    def rotate_col(self, A, B):
        """ Shifts column A down by B pixels """
        

def main():
    lcd = LCD();
    lcd.rect(3, 2)
    print(lcd)
    lcd.rotate_row(0, 4)
    print(lcd)
    print("Number of pixels lit: ",lcd.get_lit())

if __name__ == "__main__":
    main()