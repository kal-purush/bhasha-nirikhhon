import pandas as pd
from extract import extract_data  # Importer la fonction d'extraction depuis le fichier extract.py

def transform_data(data):
    # Liste des langues originales et leurs codes correspondants
    langues_codes = {
        'English': 'EN',
        'French': 'FR',
        'Chinese': 'ZH',
        'Hindi': 'HI',
        'Portuguese': 'PT',
        'Spanish': 'ES',
        'German': 'DE',
        'Italian': 'IT',
        'Norwegian': 'NO',
        'Russian': 'RU',
        'Dutch': 'NL',
        'Swedish': 'SV',
        'Japanese': 'JA',
        'Czech': 'CS',
        'Yiddish': 'YI',
        'Gujarati': 'GU'
    }

    # Ajouter une colonne au DataFrame pour le code de langue normalisé
    data['Language code'] = data['Original language'].map(langues_codes)

    return data

if _name_ == "_main_":
    extracted_data = extract_data()  # Appeler la fonction d'extraction
    transformed_data = transform_data(extracted_data)  # Appliquer la transformation
    print("Données transformées:")
    print(transformed_data.head())