import random

class Giocatore:
    def __init__ (self, nome, punteggio):
        self.nome = nome
        self.punteggio = punteggio

    def __repr__ (self):
        return f"{self.nome}"

    def __lt__(self, b):
        if self.punteggio > b.punteggio:
            return 1
        elif self.punteggio == b.punteggio:
            if self.nome.__lt__(b.nome) is True:
                return 1

class Domanda:
    def __init__ (self, testo, difficoltà, risposte):
        self. testo = testo
        self. difficoltà = difficoltà
        self.risposte = risposte



#In Python non occorre nemmeno dichiarare, negli attributi, se si tratta o meno di liste:
#self. risposte = []       risulta pertanto una dichiarazione ERRATA


    def __repr__(self):
        return f"{self.testo}{self.difficoltà}\n{self.risposte}\n\n"

file = open ('domande.txt', 'r', encoding='utf-8')
listaRighe = (file.readlines())
#se il programma viene eseguito senza chiudere il file, stranamente non viene rilevato anclun errore
file.close()

contRighe = 0
listaDomande = []
while (contRighe+5) < len(listaRighe):
    listaDomande.append(Domanda(listaRighe[contRighe], int(listaRighe[contRighe+1][0 : -1]), [listaRighe[contRighe+2], listaRighe[contRighe+3], listaRighe[contRighe+4], listaRighe[contRighe+5]]))
    contRighe += 7

print("Benvenuto in Trivia Game!\n")

punteggio = 0
sottolista = []
rispostaCorretta = True

while rispostaCorretta == True:
    for o in listaDomande:
        if o.difficoltà == punteggio:
            sottolista.append(o)
    if len(sottolista) == 0:
        print (f"Complimenti, hai raggiunto il punteggio massimo, che è {punteggio}!")
        break
    else:
        print (f"Domanda numero {punteggio+1}:\n")
        domandaCorrente = sottolista[random.randrange(0, len(sottolista))]
        print (domandaCorrente.testo, end='')
        ordineRisposte = [0, 1, 2, 3]
        random.shuffle(ordineRisposte)
        counter = 0
        while counter < 4:
            if domandaCorrente.risposte[ordineRisposte[counter]][-1] == '\n':
                print (domandaCorrente.risposte[ordineRisposte[counter]], end='')
            else: print (domandaCorrente.risposte[ordineRisposte[counter]])
            counter += 1
        flag = 0
        while flag == 0:
            rispostaUtente = input ()
            counter = 0
            for p in domandaCorrente.risposte:
                if rispostaUtente.__eq__(p[0 : -1]):
                    flag = 1
            if flag == 0:
                print ("La risposta data non è tra quelle selezionabili, dunque riprova.")
        if rispostaUtente.__eq__(domandaCorrente.risposte[0][0 : -1]):
            print ("\nBravo, risposta corretta!\n\n")
            sottolista.clear()
            punteggio += 1
        else:
            print ("Risposta sbagliata. FIne del gioco. Punteggio:", punteggio)
            rispostaCorretta = False

print ("Inserisci il tuo nickname: ", end='')
newGiocatore = Giocatore (input(), punteggio)

file = open ('punti.txt', 'r', encoding='utf-8')
listaRighe = (file.readlines())
file.close()

listaGiocatori = []
counter = 0
while counter < len(listaRighe):
    if listaRighe[counter][-1] == '\n':
        giocatoreDaInserire = Giocatore (listaRighe[counter][0:-3], int(listaRighe[counter][-2]))
    else: giocatoreDaInserire = Giocatore (listaRighe[counter][0:-2], int(listaRighe[counter][-1]))
    listaGiocatori.append(giocatoreDaInserire)
    counter += 1
listaGiocatori.append(newGiocatore)

listaGiocatori.sort()


file = open ('punti.txt', 'w', encoding='utf-8')
counter = 0
while counter < len(listaGiocatori)-1:
    file.write(f"{listaGiocatori[counter].nome} {listaGiocatori[counter].punteggio}\n")
    counter += 1
file.write(f"{listaGiocatori[len(listaGiocatori)-1].nome} {listaGiocatori[len(listaGiocatori)-1].punteggio}")
file.close()


#controllo che rispostaUtente e counter possano essere lasciati dentro il while e siano
#visibili dal resto del codice (cosa che in Java non sarebbe possibile)