class Atleta:
    def __init__(self, name, surname, age, height, weight):
        self.name = name
        self.surname = surname
        self.age = age
        self.height = height
        self.weight = weight
        self.visitaMedica = False
        self.squadra = None

    def info(self):
        print("nome:", self.name)
        print("cognome:", self.surname)
        print("età:", self.age)
        print("altezza:", self.height)
        print("peso:", self.weight)
        if self.visitaMedica == False:
            print("L'atleta", self.name, self.surname, "non ha effettuato la visita medica")
        else:
            print("L'atleta", self.name, self.surname, "ha effettuato la visita medica")

        if self.squadra is None:
            print("L'atleta", self.name, self.surname, "non è in nessuna squadra")
        else:
            print("L'atleta", self.name, self.surname, "gioca nel", self.squadra)
        
    def team(self, team_name):
        self.squadra = team_name
        print(self.name, self.surname, "ora gioca nel", team_name)
        
    def effettua_visita(self):
        if self.visitaMedica == False:
            self.visitaMedica = True
            print("La visita medica è stata effettuata da", self.name, self.surname)
        else:
            print("La visita medica è già stata effettuata")
            
atleta1 = Atleta("Pino", "Rossi", 16, 180, 70)
atleta1.effettua_visita()
atleta1.info()
atleta1.team("Napoli")
atleta1.info()