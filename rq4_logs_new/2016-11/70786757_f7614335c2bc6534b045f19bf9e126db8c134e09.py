Author = "Alban"

def str_to_sec(string):
    """
    Convertit une chaine de caractere au format HH:MM:SS en un nombre de secondes (depuis 00:00:00)
    :param string: chaine de caractere au format HH:MM:SS
    :return: Entier correspondant au nombre de secondes ecoulees depuis 00:00:00
    """
    (h,m,s)= string.split(':')
    return (int(h)*3600 + int(m)*60 + int(s))

def sec_to_str(nb_sec):
    """
    Convertit un nombre de seconde donne en une chaine au format HH:MM:SS
    :param nb_sec: nombre de seconde du temps a convertir
    :return: Chaine de caractere au format HH:MM:SS
    """
    h = nb_sec//3600
    nb_sec -= h*3600
    m = nb_sec // 60
    s = nb_sec - m*60
    return "%02d:%02d:%02d" % (h, m, s)


# print str_to_sec("13:20:50")
# print sec_to_str(str_to_sec("13:20:50"))