import logging
import os
import json
import requests
from selectolax.parser import HTMLParser
import chardet
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database file to store processed information
DB_FILE = "pets_db.json"

# Example list of dog breeds
DOG_BREEDS = [
    "Kříženec", "Afgánský chrt", "Aidi", "Akita-Inu", "Aljašský malamut", "American bully",
    "Americká akita (Velký japonský pes)", "Americký bezsrstý terier", "Americký buldok",
    "Americký foxhound", "Americký kokršpaněl", "Americký Mini-Pei", "Americký pitbulteriér",
    "Americký staford", "Americký vodní španěl", "Anatolský pastevecký pes",
    "Anglicko-francouzký honič de Petite Venerie", "Anglický buldok", "Anglický chrt (Greyhound)",
    "Anglický kokršpaněl", "Anglický mastif", "Anglický setr", "Anglický špringr španěl",
    "Anglický toy terier", "Appenzellský salašnický pes", "Argentinská doga",
    "Ariegský ohař krátkosrstý", "Artésko-normandský basset", "Artoisský basset", "Artoisský honič",
    "Australian Stumpy Tail Cattle Dog", "Australská kelpie", "Australský honácký pes",
    "Australský ovčák", "Australský Silky terier", "Australský terier", "Auvergneský ohař krátkosrstý",
    "Azavak", "Balkánský honič", "Bandog", "Barbet", "Barevný boloňský psík", "Barzoj", "Basenji",
    "Basset Hound", "Bavorský barvář", "Beagle", "Beagle Harrier", "Bearded kolie", "Beauceron",
    "Bedlington terier", "Belgický grifonek", "Belgický ovčák Groenendael", "Belgický ovčák Laekenois",
    "Belgický ovčák Malinois", "Belgický ovčák Tervueren", "Bergamský ovčák", "Bernský honič",
    "Bernský salašnický pes", "Bílá kolie", "Bílý švýcarský ovčák", "Bišonek", "Black and Tan Coonhound",
    "Bloodhound", "Bobtail", "Boloňský psík", "Bordeauxská doga", "Border kolie", "Border terier",
    "Bosenský hrubosrstý honič", "Bostonský terier", "Bourbonský ohař krátkosrstý", "Brabantík",
    "Brakýř jezevčíkovitý", "Braque Dupuy", "Brazilská fila", "Brazilský terier", "Bretaňský ohař dlouhosrstý",
    "Briard", "Briquet Griffon Vendéen", "Bruselský grifonek", "Bullmastif", "Bullterier", "Burgoský perdiquero",
    "Búrský buldok", "Cairn Terier", "Cane Corso", "Canaanský pes", "Cao de Castro Laboreiro",
    "Cao de Serra de Aires", "Čau Čau (Chow chow)", "Černý terier", "Československý vlčák", "Český fousek",
    "Český horský pes", "Český strakatý pes", "Český terier", "Čínský chocholatý pes", "Čivava", "Clumber španěl",
    "Coton de Tuléar", "Curly Coated Retriever", "Dalmatin", "Dandie Dinmont terier", "Dánská doga",
    "Dánsko-švédský farmářský pes", "Dánský ohař krátkosrstý", "Deerhound", "Dlouhosrstý ohař z Pont-Audemer",
    "Dlouhosrstý vipet", "Dobrman", "Drever", "Dunker", "Entlebuchský salašnický pes", "Erdelteriér",
    "Eskymácký pes", "Estrelský pastevecký pes", "Eurasier", "Evropský saňový pes", "Faraónský pes",
    "Field španěl", "Finský honič", "Finský špic", "Flanderský bouvier", "Flat Coated Retriever",
    "Foxhound", "Foxterier hladkosrstý", "Foxterier hrubosrstý", "Francouzský bílo-černý honič",
    "Francouzský bílo-oranžový honič", "Francouzský buldoček", "Francouzský ohař dlouhosrstý",
    "Francouzský ohař krátkosrstý gaskoňského typu", "Francouzský ohař krátkosrstý pyrenejského typu",
    "Francouzský trikolorní honič", "Frízský ohař Stabyhoun", "Gordonsetr", "Griffon á poil laineux",
    "Griffon d'Arrét á Poil Dur", "Grónský pes", "Hahoavu", "Haldenův honič", "Hamiltonův honič",
    "Hannoverský barvář", "Harrier", "Havanský psík", "Hokkaido-Ken", "Holandský ovčácký pudl", "Holandský ovčák",
    "Holandský pinč", "Hovawart", "Hrubosrstý modrý gaskoňský honiče", "Hygenův honič", "Chambray",
    "Chesapeake Bay Retriever", "Chien Fila de Saint Miguel", "Chodský pes", "Chortaja Borzaja",
    "Chorvatský ovčák", "Ibizský podenco", "Irish Glen of Imaal terier", "Irský červenobílý setr",
    "Irský soft coated wheaten teriér", "Irský setr", "Irský terier", "Irský vlkodav", "Irský vodní španěl",
    "Islandský pes", "Istrijský hrubosrstý honič", "Istrijský krátkosrstý honič", "Italský chrtík", "Italský ohař",
    "Italský Segugio", "Italský Segugio krátkosrstý", "Italský spinone", "Italský volpino", "Jack Russell Teriér",
    "Jaemthund", "Jagdteriér", "Japan-chin", "Japonský špic", "Japonský terier", "Jezevčík", "Jihoruský ovčák",
    "Jorkšírský terier", "Jorkšírský Biewer terier", "Jugoslávský planinský honič", "Jugoslávský trikolorní honič",
    "Kai-Inu", "Kanárská doga", "Kanárský podenco", "Karelský medvědí pes", "Katalánský ovčák",
    "Kavalír King Charles španěl", "Kavkazský pastevecký pes", "Kerry blue terier", "King Charles španěl",
    "Kishu-Inu", "Knírač malý", "Knírač střední", "Knírač velký", "Kolie dlouhosrstá", "Kolie krátkosrstá",
    "Komondor", "Kontinentální buldok", "Kooikerhondje", "Korejský Jindo Dog", "Krašský ovčák", "Kromforländer",
    "Kuvasz", "Labradorský retriever", "Lagotto romagnolo", "Lajka karelo-finská", "Lakeland terier", "Landseer",
    "Lapinkoira", "Lapinporokoira", "Laponský pes", "Leonberger", "Levesque",
    "Lhasa Apso", "Louisianský leopardí pes", "Lvíček", "Maďarský chrt",
    "Maďarský ohař drátosrstý", "Maďarský ohař krátkosrstý", "Malorský ovčák",
    "Maltézský psík", "Malý hrubosrstý vendéeský basset", "Malý modrý gaskoňský honič",
    "Malý münsterlandský ohař", "Manchester terier", "Maremmansko-abruzský pastevecký pes",
    "Mexický naháč", "Modrý gaskoňský basset", "Modrý pikardský ohař dlouhosrstý",
    "Mops", "Moskevský strážní pes", "Moskevský toy terier", "Mudi",
    "Neapolský mastin", "Německá doga", "Německý boxer", "Německý honič",
    "Německý křepelák", "Německý ohař dlouhosrstý", "Německý ohař drátosrstý",
    "Německý ohař krátkosrstý", "Německý ohař ostnosrstý", "Německý ovčák",
    "Německý pinč", "Německý špic", "Nivernaisský hrubosrstý honič", "Norfolk terier",
    "Normandský Poitevin", "Norský buhund", "Norský losí pes černý", "Norský losí pes šedý",
    "Norský lundehund", "Norwich terier", "Nova Scotia Duck Tolling Retriever",
    "Novofundlandský pes", "Opičí pinč", "Otterhound (Vydrař)", "Papillon",
    "Parson Russell Teriér", "Patterdale terier", "Pekingský palácový psík",
    "Perro de Presa Mallorquin", "Peruánský naháč", "Pikardský ohař dlouhosrstý",
    "Pikardský ovčák", "Plavý bretaňský basset", "Plavý bretaňský honič",
    "Podhalaňský ovčák", "Pointer", "Poitevin", "Polský chrt", "Polský ogar",
    "Polský ovčák nížinný", "Porcelaine", "Portugalský ohař", "Portugalský podengo",
    "Portugalský vodní pes", "Posávský honič", "Pražský krysařík", "Pudl",
    "Pudlpointr", "Puli", "Pumi", "Pyrenejský horský pes", "Pyrenejský mastin",
    "Pyrenejský ovčák s dlouhou srstí v obličeji", "Pyrenejský ovčák s krátkou srstí v obličeji",
    "Rafeiro do Alentejo", "Rakouský krátkosrstý honič", "Rakouský krátkosrstý pinč",
    "Řecký honič", "Rhodézský ridgeback", "Romanian Carpathian Shepherd Dog",
    "Romanian Mioritic Shepherd Dog", "Rotvajler", "Ruskoevropská lajka",
    "Ruský Toy teriér", "Saarlosův vlčák", "Saint-Germainský ohař krátkosrstý",
    "Saluki - Perský chrt", "Samojed", "Šarpej", "Šarplaninský pastevecký pes",
    "Sealyham terier", "Sedmihradský honič", "Sheltie", "Shiba-Inu", "Shih-tzu",
    "Schillerův honič", "Sibiřský husky", "Sicilský chrt", "Šiperka", "Skotský terier",
    "Skye terier", "Sloughi", "Slovenský čuvač", "Slovenský kopov",
    "Slovenský ohař hrubosrstý", "Smalandský honič", "Staroanglický Buldog",
    "Španělský galgo", "Španělský mastin", "Španělský sabueso", "Španělský vodní pes",
    "Stafordšírský bullterier", "Středoasijský pastevecký pes", "Štýrský brakýř",
    "Sussex španěl", "Svatobernarský pes", "Švýcarský honič",
    "Švýcarský nízkonohý honič", "Thajský ridgeback", "Tibetská doga", "Tibetský španěl",
    "Tibetský terier", "Tosa-Inu", "Tornjak", "Trpasličí pinč", "Tyrolský honič",
    "Uruguayský cimarron", "Velký francouzko-anglický bílo-černý honič",
    "Velký francouzko-anglický bílo-oranžový honič", "Velký francouzko-anglický trikolorní honič",
    "Velký gaskoňsko-saintongeoisský honič", "Velký hrubosrstý vendéeský basset",
    "Velký modrý gaskoňský honič", "Velký münsterlandský ohař", "Velký švýcarský salašnický pes",
    "Velký vendéeský hrubosrstý honič", "Vipet", "Východosibiřská lajka",
    "Výmarský ohař", "Welsh Corgi Cardigan", "Welsh Corgi Pembroke", "Welsh terier",
    "Welššpringršpaněl", "Westfálský jezevčíkovitý honič", "West Highland White Terrier",
    "Západosibiřská lajka", "Zlatý retriever"
]

def load_database(db_file: str) -> dict:
    """Load the database from a JSON file, or return an empty dictionary if not found."""
    if os.path.exists(db_file):
        logging.info("Loading database from %s", db_file)
        with open(db_file, 'r', encoding='utf-8') as file:
            return json.load(file)
    logging.warning("Database file %s not found. Creating a new one.", db_file)
    return {}

def save_database(db_file: str, data: dict):
    """Save the database to a JSON file."""
    logging.info("Saving data to %s", db_file)
    with open(db_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def get_page_content(url: str) -> str:
    """Fetch the page content and ensure proper encoding."""
    response = requests.get(url)
    detected_encoding = chardet.detect(response.content)['encoding']
    response.encoding = detected_encoding
    return response.text

def scrape_page(url: str):
    """Scrape the given URL for links to dog breeds."""
    try:
        html = get_page_content(url)
        tree = HTMLParser(html)
        catalog_items = tree.css('div.catalog-item')

        logging.info("Found %d 'div.catalog-item' elements.", len(catalog_items))
        dogs = []

        # Assuming catalog_items is a list of 'div.catalog_item' elements
        for item in catalog_items:
            # Find the 'div.txt' element inside the catalog_item
            txt_div = item.css('div.txt')
            url = item.css_first('a').attributes.get('href')

            if txt_div:
                # Within 'div.txt', find all nested 'div' elements
                nested_divs = txt_div[0].css('div')

                if len(nested_divs) > 1:
                    # Get the text of the second 'div' inside the 'div.txt'
                    second_div_text = nested_divs[2].text()
                    plemeno = re.search(r'(?<=:\s)(.*)', second_div_text).group(1)

                    # Check if the text from the second div is in the predefined list
                    if plemeno in DOG_BREEDS:
                        dogs.append(url)
        for dog in dogs:
            logging.info("Visiting %s", dog)
            scrape_images_and_details(dog)
    except requests.exceptions.RequestException as e:
        logging.error("Error scraping %s: %s", url, e)

def scrape_images_and_details(url: str):
    """Scrape the details and images of a specific pet."""
    try:
        html = get_page_content(url)
        tree = HTMLParser(html)

        pet_details = {}

        # Find all image elements
        images = tree.css('div.thumbs a.lg-trigger')
        # Find all detail elements
        pet_details_grid = tree.css('div.product-bottom div.uk-grid div.line')

        # Extract image URLs and save the images
        if len(images) >= 2:
            for detail in pet_details_grid:
                # Get the key (label) from the span
                span_element = detail.css_first('span')
                key = span_element.text().strip() if span_element else "Unknown Key"

                # Get the full text of the detail (key: value)
                value = detail.text().strip()

                # Split the value by the colon or whitespace, and take the part after the key
                parts = value.split(" ", 1)  # Split only once by the first colon
                if len(parts) > 1:
                    # Extract the part after the colon and strip extra spaces
                    pet_details[key] = parts[1].strip()
                else:
                    # If no colon is found, save the full text as value
                    pet_details[key] = value

            pet_details['url'] = url
            pet_details['images'] = []
            for img in images:
                img_url = img.attributes.get('href')
                if img_url:
                    pet_details['images'].append(img_url)

            save_to_json(pet_details, DB_FILE)
    except Exception as e:
        logging.error("Error scraping details from %s: %s", url, e)

def save_to_json(data_dict: dict, db_path: str):
    """Save pet details to a JSON file with a unique identifier."""
    try:
        pet_id = re.search(r'([^/]+)$', data_dict['url']).group(1) if re.search(r'([^/]+)$', data_dict['url']) else "unknown_id"
        db = load_database(db_path)
        db[pet_id] = data_dict
        save_database(db_path, db)

        logging.info("Successfully saved pet details with ID '%s'", pet_id)
    except Exception as e:
        logging.error("Error saving to JSON: %s", e)

def main():
    """Main function to execute the scraping process."""
    # Generate URLs for scraping based on page numbers
    urls_to_scrape = [f"https://www.psidetektiv.cz/ztracena-zvirata/strana/{x}/" for x in range(264,596)]

    # Process each URL in the list
    for url in urls_to_scrape:
        logging.info("Starting to scrape %s", url)
        scrape_page(url)


if __name__ == "__main__":
    main()