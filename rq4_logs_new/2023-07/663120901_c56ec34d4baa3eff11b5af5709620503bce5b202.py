import sqlite3

connexion = sqlite3.connect("car.db")
curseur = connexion.cursor()

###Exécution unique
curseur.execute('''CREATE TABLE IF NOT EXISTS Voiture(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,nom TEXT,couleur TEXT,vitesse_max INTEGER)''')

connexion.commit()

"""
### Insertion d'une donnée
donnee = ("Toyota", "rouge", 2000)
curseur.execute('''INSERT INTO Voiture (nom, couleur, vitesse_max) VALUES (?, ?, ?)''',donnee)
connexion.commit()
"""

### Insertion multiple de données
donnees = (
    {"nom" : "Toyota", "couleur" : "rouge", "vitesse_max" : 2000},
    {"nom" : "Mercedes", "couleur" : "rouge", "vitesse_max" : 2500},
    {"nom" : "Suzuki", "couleur" : "orange", "vitesse_max" : 2000},
    {"nom" : "BMW", "couleur" : "rouge", "vitesse_max" : 3000},
    {"nom" : "Toyota", "couleur" : "bleue", "vitesse_max" : 1600},
    {"nom" : "Audi", "couleur" : "noire", "vitesse_max" : 5000}
    )
curseur.executemany("INSERT INTO Voiture (nom,couleur, vitesse_max) VALUES (:nom, :couleur, :vitesse_max)", donnees)
connexion.commit()

# Sélectionner un item dans la DB via son pseudo
donnee = ("rouge", )
curseur.execute("SELECT nom FROM Voiture WHERE couleur = ?",donnee)
result = curseur.fetchone()
print(result)
while result:
    print(result)
    result = curseur.fetchone()
 
connexion.close()