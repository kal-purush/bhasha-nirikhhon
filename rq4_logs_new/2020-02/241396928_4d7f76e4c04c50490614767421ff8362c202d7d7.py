import time
import os


qa = []
qb = []
qc = []


class Puf:
    def __init__(self, no, position,):

        self.no = no
        self.position = position
        self.qa = []
        self.qb = []
        self.qc = []



    def choice(self, n):
        if n == 'A' or n == 'a':
            self.qa.append([])
            x == 'Twój numer to ' + (self.qa[+1])
            return x



if __name__ == "__main__":

    while 1 == 1:
        print('Witamy w urzędzie: A - rejestracja pojazdów' +str(qa)+ ' B - odbiór prawo jazdy' +str(qb)+ 'C - odbiór dowodu osobistego' +str(qc))
        n = input('podaj litrę: ')
        print(Puf.choice(no))
        time.sleep(3)
        #os.system('cls') # nie działa, dlaczego?