with open("shakespeare.txt", "r") as f:
    lines = f.read().splitlines()  # toutes les lignes dans une liste
    nb_lines = len(lines)
    nb_chars = 0
    nb_words = 0
    for line in lines:
        # traitement de la ligne
        for char in line:
            # traitement de chaque caractère
            # utilisation de isalnum
            nb_chars += 1