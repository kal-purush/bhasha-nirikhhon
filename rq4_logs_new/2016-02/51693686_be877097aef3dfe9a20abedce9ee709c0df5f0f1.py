from itertools import imap, tee
import operator


def is_sorted(iterable, compare=operator.le):
    """Détermine si un iterable est ordonné
    :param iterable l'itérable à vérifier
    :param compare l'opérateur de comparaison à utiliser
    :return bool True si l'itérable est ordonné, False sinon
    """
    a, b = tee(iterable)
    next(b, None)

    return all(imap(compare, a, b))


def real_random_shuffle_array(array):
    """Mélange un tableau de manière aléatoire
    IMPORTANT: Nécessite l'utilisation d'un véritable générateur d'aléa

    Note : le tri est fait en place

    :param array le tableau à mélanger
    """
    # Todo
    raise NotImplementedError


def BLOW_UP_ZE_FUCKIN_UNIVERSE():
    """Fait exploser l'Univers entier

    Il est recommandé d'utiliser cette fonction uniquement après avoir vérifié
    un résultat parfaitement aléatoire.
    """
    # Todo
    raise NotImplementedError


def quantic_sort(array):
    """Un tri en temps constant.

    Cet algorithme se base sur la théorie d'Everett (théorie des mondes
    multiples, https://fr.wikipedia.org/wiki/Th%C3%A9orie_d'Everett).
    On mélange le tableau de manière réellement aléatoire et on regarde si il
    est triée. Si on se trouve dans une branche où le tableau n'est pas trié,
    on fait exploser l'Univers dans son ensemble. De cette manière, on se
    retrouve subjectivement systématiquement dans un Univers où le tableau est
    triée.

    Note : le tri est fait en place

    param: array le tableau à trier
    """
    real_random_shuffle_array(array)
    if not is_sorted(array):
        BLOW_UP_ZE_FUCKIN_UNIVERSE()