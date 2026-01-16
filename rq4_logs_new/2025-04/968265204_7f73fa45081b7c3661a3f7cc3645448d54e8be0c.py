import winreg
import codecs
import os

# Chemin précis vers le dossier TP3 situé dans le dossier SEC102
dossier_tp3 = "./SEC102/TP3"
os.makedirs(dossier_tp3, exist_ok=True)

# Fonction de lecture de la base de registre
def lire_userassist():
    chemin_base = r"Software\Microsoft\Windows\CurrentVersion\Explorer\UserAssist"
    clé_hcu = winreg.HKEY_CURRENT_USER

    # Fichier userassist.txt dans SEC102/TP3
    chemin_userassist = os.path.join(dossier_tp3, "userassist.txt")
    with open(chemin_userassist, "w", encoding='utf-8') as f:
        try:
            with winreg.OpenKey(clé_hcu, chemin_base) as cle:
                nb_sous_cles, _, _ = winreg.QueryInfoKey(cle)
                for i in range(nb_sous_cles):
                    sous_cle_nom = winreg.EnumKey(cle, i)
                    with winreg.OpenKey(cle, sous_cle_nom + r"\Count") as sous_cle:
                        nb_valeurs = winreg.QueryInfoKey(sous_cle)[1]
                        for j in range(nb_valeurs):
                            nom_valeur, _, _ = winreg.EnumValue(sous_cle, j)
                            f.write(nom_valeur + "\n")
            print("userassist.txt a été créé avec succès dans SEC102/TP3.")
        except Exception as e:
            print("Erreur lors de la lecture de la base de registre :", e)

# Fonction de décodage ROT13
def decode_rot13():
    chemin_userassist = os.path.join(dossier_tp3, "userassist.txt")
    chemin_decode_userassist = os.path.join(dossier_tp3, "decode_userassist.txt")

    try:
        with open(chemin_userassist, "r", encoding='utf-8') as fichier_in, \
             open(chemin_decode_userassist, "w", encoding='utf-8') as fichier_out:
            for ligne in fichier_in:
                ligne_decodee = codecs.decode(ligne.strip(), 'rot_13')
                fichier_out.write(ligne_decodee + "\n")
        print("decode_userassist.txt a été créé avec succès dans SEC102/TP3.")
    except Exception as e:
        print("Erreur lors du décodage ROT13 :", e)

# Exécution des fonctions
lire_userassist()
decode_rot13()
import winreg
import codecs

# Fonction de lecture de la base de registre
def lire_userassist():
    chemin_base = r"Software\Microsoft\Windows\CurrentVersion\Explorer\UserAssist"
    clé_hcu = winreg.HKEY_CURRENT_USER

    # Création automatique du fichier userassist.txt
    with open("userassist.txt", "w", encoding='utf-8') as f:
        try:
            with winreg.OpenKey(clé_hcu, chemin_base) as cle:
                nb_sous_cles, _, _ = winreg.QueryInfoKey(cle)
                for i in range(nb_sous_cles):
                    sous_cle_nom = winreg.EnumKey(cle, i)
                    with winreg.OpenKey(cle, sous_cle_nom + r"\Count") as sous_cle:
                        nb_valeurs = winreg.QueryInfoKey(sous_cle)[1]
                        for j in range(nb_valeurs):
                            nom_valeur, _, _ = winreg.EnumValue(sous_cle, j)
                            f.write(nom_valeur + "\n")
            print("userassist.txt a été créé avec succès.")
        except Exception as e:
            print("Erreur lors de la lecture de la base de registre :", e)


# Exécution des fonctions
lire_userassist()
import winreg
import codecs


# Fonction de décodage ROT13
def decode_rot13():
    try:
        with open("userassist.txt", "r", encoding='utf-8') as fichier_in, \
             open("decode_userassist.txt", "w", encoding='utf-8') as fichier_out:
            for ligne in fichier_in:
                ligne_decodee = codecs.decode(ligne.strip(), 'rot_13')
                fichier_out.write(ligne_decodee + "\n")
        print("decode_userassist.txt a été créé avec succès.")
    except Exception as e:
        print("Erreur lors du décodage ROT13 :", e)

# Exécution des fonctions
decode_rot13()