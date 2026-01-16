import requests

# URL de l'API
url = 'http://127.0.0.1:8000/'

# Paramètres de la requête
params = {
    'store_name': 'Marjane',
    'year': 2023,
    'month': 9,
    'day': 5,
    'sensor_id': 5
}

try:
    # Envoi de la requête GET
    response = requests.get(url, params=params)

    # Vérification du code de statut
    if response.status_code == 200:
        # Extraction des données (supposant que la réponse est au format JSON)
        data = response.json()
        print(data)
    else:
        print(f"Erreur lors de la requête: {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"Une erreur s'est produite: {e}")