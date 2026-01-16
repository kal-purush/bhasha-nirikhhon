author = 'Alban'

import Models as mod
from sqlalchemy.orm import sessionmaker, query

Session = sessionmaker(bind=mod.engine)

def line_search(filename, beginning_pattern, ending_pattern):
    """
    Extrait une liste de lignes comprises entre deux motifs (non inclus) a partir d'un fichier texte passe en parametre
    :param filename: fichier texte contenant les lignes a extraire
    :param beginning_pattern: motif indiquant le debut des lignes utiles
    :param ending_pattern: motif indiquant la fin des lignes utiles
    :return: list_lines: liste de chaine de caracteres correspondant aux lignes utiles (un element = une ligne)
    """
    with open(filename, 'r') as f:
        (start, end) = (0, 0)
        lines = f.readlines()
        for (i, line) in enumerate(lines):
            if beginning_pattern in line:
                start = i+1
            if ending_pattern in line:
                end = i
        list_lines = lines[start:end]
    return list_lines

def f_import_beacons(filename):
    """
    Importe les donnees relative aux balise a partir d un fichier texte passe en entree et les stocke dans la BDD
    :param filename: fichier texte contenant les balises a importer
    :return: 0 si correctement importe, 1 sinon
    """
    l_beacons = line_search(filename, "NBeacons:", "########## Layers")

    if l_beacons == []:
        print("Aucune balise dans le fichier %s") % (filename)
        return 1

    for beacon in l_beacons:
        (b_name, b_x_pos, b_y_pos) = beacon.split()[0:3]                  #Attention: la position est une str ici, pas un entier
        tmp_beacon = mod.Beacon(name=b_name, pos_x=int(b_x_pos), pos_y=int(b_y_pos))
        #Remplissage des tables avec les balises
        tmp_session = Session()
        if (tmp_session.query(mod.Beacon).filter(mod.Beacon.name==b_name).first()) == None :
            #Ajout de la balise a la bdd si elle n'y est pas deja
            tmp_session.add(tmp_beacon)
        else:
            #Cas ou une balise portant le meme nom est dans la bdd
            conflicting_beacon = tmp_session.query(mod.Beacon).filter(mod.Beacon.name == b_name).first()
            if conflicting_beacon is not tmp_beacon:
            #Modification des champs si differents
                if conflicting_beacon.pos_x != b_x_pos:
                    conflicting_beacon.pos_x = b_x_pos
                elif conflicting_beacon.pos_y != b_y_pos:
                    conflicting_beacon.pos_y = b_y_pos
        tmp_session.commit()
    return 0

def f_import_flights(filename):
    """
    Importe les donnees relative aux vols a partir d un fichier texte passe en entree et les stocke dans la BDD
    :param filename: fichier texte contenant les vols a importer
    :return:
    """

print("Importation des balises ...")
f_import_beacons("../exemple_donnees.txt")
print("Fait")
#session_test = Session()
#test_beacon = session_test.query(mod.Beacon).first()
#print test_beacon
#session_test.commit()