import sys

class pagePrints:
    def __init__(self,cols,rows):
        self.cols=cols
        self.rows=rows
        sys.stdout.write("\x1b[8;{rows};{cols}t".format(rows=self.rows, cols=self.cols))

    def prentButton(self,bil,merki,ord):
        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        for _ in range(merki):
            print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#")

        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#",end=" ")
        for _ in range(merki-2):
            print(" ",end=" ")
        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#",end=" ")

        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#",end=" ")
        print(ord,end="  ")
        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#")

        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#",end=" ")
        for _ in range(merki-2):
            print(" ",end=" ")
        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#")

        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        for _ in range(merki):
            print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print("#")

    def prentSpace(self,linur,breidd):
        breidd=int(breidd)
        for _ in range(linur):
            print("#",end=" ")
            for _ in range(breidd-2):
                print(" ",end=" ")
            print("#",end=" ")


    def prentLina(self,breidd):
        breidd=int(breidd)
        for _ in range(breidd):
            print("#",end=" ")
        print()

    def prentWord(self,bil,ord):
        print("#",end=" ")
        for _ in range(bil):
            print(" ",end=" ")
        print(ord,end=" ")
        for _ in range(80-2-2*bil-len(ord)):
            print(" ",end=" ")
        print("#")

    def frontPage(self):
        self.prentLina((self.cols/2))
        self.prentSpace(2,(self.cols/2))
        self.prentButton(13,12,"    Create new    ")
        self.prentButton(13,12,"  Get information ")
        self.prentButton(13,12,"      Update      ")
        self.prentSpace(2,40)
        self.prentWord(7,"Please choose ['C' create, 'G' get, 'U' update]")
        self.prentSpace(1,40)
        self.prentLina(40)

    def window1(self):
        self.prentLina(40)
        self.prentSpace(8,40)
        self.prentWord(14,"1: New staffmember ")
        self.prentWord(14,"2: New voyage      ")
        self.prentWord(14,"3: New destination ")
        self.prentWord(14,"4: New airplane    ")
        self.prentSpace(7,40)
        self.prentWord(12,"Please choose [1,2,3 or 4] ")
        self.prentSpace(1,40)
        self.prentLina(40)