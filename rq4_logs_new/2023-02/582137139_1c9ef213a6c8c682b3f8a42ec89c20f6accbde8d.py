class Pola:
    global matryca
    def fA(X,Y):
        if fields.field_A == 'empty':
           matryca[X][Y]=Przyciski.pisana_nazwa_piona
           Przyciski.set_pion(-150,175)
           print(matryca)
        else:
            print ('To pole jest zajęte')
            

    def fB(X,Y):
        if fields.field_B == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(0,175)
        else:
            print ('To pole jest zajęte')

    def fC(X,Y):
        if fields.field_C == 'empty':
           matryca[X][Y]=Przyciski.pisana_nazwa_piona
           Przyciski.set_pion(150,175)
        else:
            print ('To pole jest zajęte')

    def fD(X,Y):
        if fields.field_D == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(-150,25)
        else:
            print ('To pole jest zajęte')

    def fE(X,Y):
        if fields.field_E == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(0,25)
        else:
            print ('To pole jest zajęte')

    def fF(X,Y):
        if fields.field_F == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(150,25)
        else:
            print ('To pole jest zajęte')

    def fG(X,Y):
        if fields.field_G == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(-150,-125)
        else:
            print ('To pole jest zajęte')

    def fH(X,Y):
        if fields.field_H == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(0,-125)
        else:
            print ('To pole jest zajęte')
            
    def fI(X,Y): 
        if fields.field_I == 'empty':
            matryca[X][Y]=Przyciski.pisana_nazwa_piona
            Przyciski.set_pion(150,-125)
        else:
            print ('To pole jest zajęte')