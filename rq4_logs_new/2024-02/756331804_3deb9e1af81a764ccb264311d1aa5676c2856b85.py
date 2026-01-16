import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL de la page du combattant
url = 'https://www.ufc.com/athlete/israel-adesanya'

# Effectuer la requête HTTP pour obtenir le contenu de la page
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Initialiser le dictionnaire principal pour recueillir toutes les données
fighter_data = {}

# Nom du combattant
fighter_data['Name'] = soup.find('h1', class_='hero-profile__name').get_text(strip=True) if soup.find('h1', class_='hero-profile__name') else 'N/A'

# Fonction pour extraire et traiter les blocs de statistiques
def process_stat_blocks(soup, fighter_data):
    stats_blocks = soup.find_all('div', class_='c-stat-3bar c-stat-3bar--no-chart')
    for block in stats_blocks:
        title = block.find('h2', class_='c-stat-3bar__title').text.strip()
        stat_groups = block.find_all('div', class_='c-stat-3bar__group')
        for group in stat_groups:
            method = group.find('div', class_='c-stat-3bar__label').text.strip()
            value = group.find('div', class_='c-stat-3bar__value').text.strip()
            fighter_data[f"{title} - {method}"] = value

    return fighter_data

fighter_data = process_stat_blocks(soup, fighter_data)

# Extraire des informations sur les coups et les ajouter à fighter_data
for target_group in soup.find_all('g', id=lambda x: x and x.endswith('-txt')):
    target = target_group.find_all('text')[-1].get_text(strip=True)
    value = target_group.find('text', id=lambda x: x and x.endswith('_value')).get_text(strip=True)
    percent = target_group.find('text', id=lambda x: x and x.endswith('_percent')).get_text(strip=True)
    fighter_data[f"{target} - Nombre de coups"] = value
    fighter_data[f"{target} - Pourcentage"] = percent

# Extraire des informations sur les comparaisons de statistiques
stat_compare_blocks = soup.find_all('div', class_='c-stat-compare')
for block in stat_compare_blocks:
    stat_groups = block.find_all('div', class_='c-stat-compare__group')
    for group in stat_groups:
        stat_label = group.find('div', class_='c-stat-compare__label').get_text(strip=True)
        stat_number = group.find('div', class_='c-stat-compare__number').contents[0].strip()
        stat_value = f"{stat_number}%" if group.find('div', class_='c-stat-compare__percent') else stat_number
        fighter_data[stat_label] = stat_value

# Extraire des informations supplémentaires et les ajouter à fighter_data
stat_containers = soup.find_all('div', class_='c-overlap__inner') + soup.find_all('div', class_='athlete-stats__stat')
for container in stat_containers:
    stat_text = container.find('p', class_='athlete-stats__text athlete-stats__stat-text').get_text(strip=True) if container.find('p', class_='athlete-stats__text athlete-stats__stat-text') else container.find('h2', class_='e-t3').get_text(strip=True)
    stat_number = container.find('p', class_='athlete-stats__text athlete-stats__stat-numb').get_text(strip=True) if container.find('p', class_='athlete-stats__text athlete-stats__stat-numb') else '0'
    fighter_data[stat_text] = stat_number if stat_number else '0'

# Convertir le dictionnaire final en DataFrame
df_final = pd.DataFrame([fighter_data])

ighter_info = {}

# Extraction des données
fields = soup.find_all('div', class_='c-bio__field')
for field in fields:
    label = field.find('div', class_='c-bio__label').get_text(strip=True)
    text = field.find('div', class_='c-bio__text').get_text(strip=True)
    
    # Mapper les labels français vers les noms des colonnes en anglais pour la clarté
    label_mapping = {
        'Lieu de naissance': 'Birthplace',
        'Âge': 'Age',
        'La Taille': 'Height',
        'Poids': 'Weight',
        'Reach': 'Reach',
        'Portée de la jambe': 'Leg Reach'
    }

    if label in label_mapping:
        fighter_info[label_mapping[label]] = text

# Créer un DataFrame à partir du dictionnaire
df_fighter_info = pd.DataFrame([fighter_info])

DF = pd.concat([df_final, df_fighter_info], axis=1)