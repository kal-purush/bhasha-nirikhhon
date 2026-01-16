# -*- coding: utf-8 -*-
# CLT Abschlussprojekt Sprachidentifikation
# Python 2.7.3
# Lisanne Wiengarten Matrikelnummer 764870

import sys
from collections import deque
from operator import itemgetter

# Untersucht das übergebene Dokument Zeile für Zeile statt im Ganzen.
# Für jede Zeile kann angegeben werden, in welcher Sprache sie wahrscheinlich ist.
# Außerdem wird ausgegeben, wie viel Prozent der Zeilen welcher Sprache
# zugeordnet wurden. Kennt der User die Sprache seines Dokuments, kann so
# überprüft werden wie gut die zeilenweise Identifikation funktioniert.

# Der Klasse Recognize_Lang wird ein Dokument übergeben, dessen
# Sprache identifiziert werden soll.
# Dazu erstellt die Klasse Profile für dieses Dokument, sowie für
# die Sprachen Deutsch, Englisch und Französisch
# (diese werden intern erstellt, der User übergibt nur das Dokument).
# Das Profil des Dokuments wird mit den Profilen der drei Sprachen
# verglichen und das mit der geringsten Distanz wird zurückgegeben
class Recognize_Lang:

    # Das zu untersuchende Objekt wird übergeben
    # Daraus wird ein Profil erstellt, das Instanzvariable der Klasse ist
    def __init__(self, untersuchungsobjekt):
        # Hier wird das n für die n-Gramme festgelegt
        # Ist ebenfalls Instanzvariable der Klasse
        self.n = 2
        self.untersuch = self.make_profile(self.n, untersuchungsobjekt)

        # Die Profile für die drei Sprachen werden angelegt
        eng_raw = open('english.txt').read()
        eng_profil = self.make_profile(self.n, eng_raw)
        
        fr_raw = open('french.txt').read()
        fr_profil = self.make_profile(self.n, fr_raw)

        ger_raw = open('german.txt').read()
        ger_profil = self.make_profile(self.n, ger_raw)

        # Für welchen Prozentsatz von Zeilen liegt das System richtig?
        counter = 0
        eng_count = 0
        fr_count = 0
        ger_count = 0
        # Das zu untersuchende Dokument wird zusätzlich zeilenweise untersucht
        # Für jede Zeile wird ein Profil angelegt das verglichen wird
        for line in untersuchungsobjekt.split("\n"):
            counter += 1
            print "Neue Zeile wird untersucht"
            profil = self.make_profile(self.n, line)
            Ergebnis = self.compare(profil, eng_profil, fr_profil, ger_profil)
            # je nach dem welche Sprache dieser Zeile zugeordnet wurde
            # wird der entsprechende Zähler hochgesetzt
            if Ergebnis == eng_profil:
                eng_count += 1
            elif Ergebnis == fr_profil:
                fr_count += 1
            elif Ergebnis == ger_profil:
                ger_count += 1
            else:
                print "Die Sprache konnte nicht ermittelt werden!"

        # Die Zähler werden durch die Gesamtzahl der Zeilen geteilt
        # und zeigen (mit 100 multipliziert) die Prozentzahlen an,
        # wie viele Zeilen welcher Sprache zugeordnet wurden
        print "Prozent an Zeilen in Englisch: ", float(eng_count)/counter * 100,"%"
        print "Prozent an Zeilen in Franz\xf6sisch: ", float(fr_count)/counter * 100,"%"
        print "Prozent an Zeilen in Deutsch: ", float(ger_count)/counter * 100,"%"

    # Funktion die aus einem Text n-Gramme erstellt
    def create_ngrams(self, n, corpus):
        return_list = list()                
        last_words = deque()                

        for i in range(n-1):
            last_words.append("<BOS>")
            
        for token in corpus:
            ngram_list = list(last_words)
            ngram_list.append(token)
            ngram = tuple(ngram_list)
                
            return_list.append(ngram)            
            if len(last_words) > 0:
                last_words.popleft()
                last_words.append(token)
                    
        for i in range(n-1): 
            ngram_list = list(last_words)
            ngram_list.append("<EOS>")
            ngram = tuple(ngram_list)
                
            return_list.append(ngram)
                    
            if len(last_words) > 0:
                last_words.popleft()
                last_words.append("<EOS>")              
        return return_list


    # Hilfsfunktion für Frequenzliste
    # Fügt ein Wort einem Dictionary hinzu wenn es noch nicht enthalten ist
    # oder erhöht die absolute Häufigkeit um 1 wenn es schon enthalten ist
    def add(self, dictionary, word):
        if word in dictionary:
            dictionary[word] = dictionary[word] + 1
        else:
            dictionary[word] = 1
            
    # Dieser Funktion wird die Liste der n-Gramme übergeben
    # daraus wird ein Histogramm, bzw. eine Frequenztabelle erstellt
    # Rückgabewert ist ein Dictionary aus n-Gramm und seiner
    # absoluten Häufigkeit im Text (bzw. in der Liste)
    def make_freqtable(self, ngramme):
        histogram = dict()
        for word in ngramme:
            self.add(histogram, word)
        return histogram

    # Diese Funktion überträgt die Frequenztabelle in ein anderes Dictionary:
    # Aus den absoluten Häufigkeiten werden relative
    # Rückgabewert ist ein Dictionary aus n-Gramm und Rang
    def fill_dict(self, freqdict):
        profil = dict()
        max_rank = len(freqdict)

        # Geht durch das Eingabe-Dict das nach absteigender Häufigkeit
        # sortiert ist
        # die Value ist dabei beim häufigsten Wort der maximale Rang
        # danach wird absteigend weitergezählt
        for word in sorted(freqdict, key=freqdict.get, reverse=True):
            freqdict[word] = max_rank
            max_rank -= 1
        return freqdict

    # Erstellt ein Profil für n-Gramme einer Sprache
    # Nimmt dafür einen Text und das n für die n-Gramme
    # Rückgabewert ist ein Dictionary aus n-Gramm und Rang (der Häufigkeit)
    # In der Funktion wird die Arbeit in kleinere Funktionen unterteilt
    # damit es übersichtlicher ist
    # (greift auf create_ngrams, make_freqtable und fill_dict zu)
    def make_profile(self, n, text):
        # aus dem gegebenen Text n-Gramme erstellen
        ngramme = self.create_ngrams(n, text)
        # aus den n-Grammen Frequenztabelle mit absoluten Häufigkeiten erstellen
        freqs = self.make_freqtable(ngramme)
        # daraus das Profil aus n-Gramm und Rang erstellen
        profil = self.fill_dict(freqs)
        return profil
        
    # Gibt die Anzahl der Einträge im gegebenen Dictionary zurück
    def max_rank(self, dictionary):
        return len(dictionary)

    # Nimmt die drei Profile der bereits bekannten Sprachen
    # und vergleicht diese mit dem Untersuchungsobjekt
    # Rückgabewert ist das Profil mit der kleinsten Distanz zum Dokument
    def compare(self, untersuch, profil1, profil2, profil3):
        profilliste = [profil1, profil2, profil3]
        profil_vergleich = list()
        wert_vergleich = list()
        
        # Der Vergleich wird für alle drei Profile durchgeführt
        for profil in profilliste:
            wert = 0
            wertliste = list()
            # Durch alle n-Gramme von untersuch gehen
            for ngramm in sorted(untersuch, key=untersuch.get, reverse=True):  
                # Prüfen ob dieses n-Gramm auch in dem anderen Profil vorhanden ist
                if ngramm in profil:
                    # wenn ja, rechne den Rang vom n-Gramm in untersuch minus
                    # den Rang vom n-Gramm im anderen Profil
                    wert = untersuch[ngramm] - profil[ngramm]
                    # jeweils den Betrag der Differenz nehmen
                    wertliste.append(abs(wert))
                else:
                    # wenn nicht vorhanden, rechne Rang vom n-Gramm in
                    # untersuch minus den maximalen Rang des anderen Profils + 1
                    wert = untersuch[ngramm] - self.max_rank(profil) + 1
                    # jeweils den Betrag der Differenz nehmen
                    wertliste.append(abs(wert))

            # Wenn ein Profil untersucht wurde, wird es in die Liste
            # profil_vergleich eingetragen
            # das dazugehörige Distanzmaß wird in die Liste wert_vergleich eingetragen
            profil_vergleich.append(profil)
            wert_vergleich.append(sum(wertliste))
                
        gesamt = sum(wert_vergleich)
        # Distanz zu jeder Sprache für jede Zeile
        print "Distanz zur Sprache Englisch:", float(wert_vergleich[0]) / gesamt * 100,"%"
        print "Distanz zur Sprache Franz\xf6sisch:", float(wert_vergleich[1]) / gesamt * 100,"%"
        print "Distanz zur Sprache Deutsch:", float(wert_vergleich[2]) / gesamt * 100,"%"        

        # Rückgabewert ist das Dict in profil_vergleich an der kleinsten
        # Stelle in wert_vergleich
        return profil_vergleich[wert_vergleich.index(min(wert_vergleich))]



# Kommandozeilenargumente importieren
if len(sys.argv) != 2:
    print 'Wrong argument number!'

# Die übergebene Datei wird in einen String ('data') umgewandelt
file = open(sys.argv[1],'r')
data = ""
while 1:
    line = file.readline()
    if not line:break
    data += line
file.close()

# Dieser String wird einer Instanz der Klasse übergeben
Recognizer = Recognize_Lang(data)