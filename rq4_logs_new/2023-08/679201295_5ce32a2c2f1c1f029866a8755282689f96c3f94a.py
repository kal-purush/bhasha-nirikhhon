import pandas as pd

def extract_data():
    path = 'C:\\Users\\User\\3D Objects\\DN\\book'
    nom_fichier_csv = 'books.csv'
    chemin_fichier_csv = path + '\\' + nom_fichier_csv

    # Spécifier l'encodage
    encodage = 'ISO-8859-1'

    # Charger le fichier CSV dans un DataFrame avec l'encodage spécifié
    data = pd.read_csv(chemin_fichier_csv, encoding=encodage)
    return data

if __name__ == "__main__":
    extracted_data = extract_data()
    print("Données extraites:")
    print(extracted_data.head())