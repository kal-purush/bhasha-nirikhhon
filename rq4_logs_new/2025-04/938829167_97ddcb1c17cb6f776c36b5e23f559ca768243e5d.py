import json
import os
import time  # Aggiunto per il delay
import requests
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
import os
from github import Github
import zipfile

load_dotenv()

class Scraper:
    def __init__(self):
        # Caricamento delle variabili d'ambiente
        self.__TEMPORARY_FOLDER = os.getenv("TEMPORARY_FOLDER")
        self.__FILE_ENDPOINT = os.getenv("FILE_ENDPOINT")
        self.__DOMAIN_FILE = os.getenv("DOMAIN_FILE")
        self.__GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
        self.__GITHUB_REPO = os.getenv("GITHUB_REPO_NAME")
        
        # Configurazione headers per evitare rilevamento bot
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.regione.marche.it/',
            'DNT': '1'
        })

    async def get_json_data(self):
        """Recupera i dati JSON dall'endpoint configurato"""
        try:
            with open(self.__FILE_ENDPOINT, 'r') as file:
                data = json.load(file)
                return json.dumps(data)
            #response = requests.get(self.__FILE_ENDPOINT, headers=self.session.headers)
            #if response.status_code == 200:
            #    return response.content 
            #else:
            #    raise Exception(f"Errore nella richiesta: {response.status_code} - {response.text}")
        except Exception as e:
            raise Exception(f"Errore nella lettura del file JSON: {str(e)}")
    
    async def download_file(self, url):
        """Scarica un file da un URL e lo salva in una cartella temporanea"""
        parsed_url = urlparse(url)
        if not parsed_url.netloc:
            # Se l'URL è relativo, aggiunge il dominio ENDPOINT_PDF_URL
            if not self.__DOMAIN_FILE:
                raise ValueError("ENDPOINT_PDF_URL non è configurato nell'ambiente.")
            url = urljoin(self.__DOMAIN_FILE, url)
            parsed_url = urlparse(url)
        
        try:
            # Check if it's a YouTube or video platform link
            video_platforms = ["youtube.com", "youtu.be", "vimeo.com", "dailymotion.com"]
            if any(platform in parsed_url.netloc for platform in video_platforms):
                return None
            
            # Create temporary folder if it doesn't exist
            os.makedirs(self.__TEMPORARY_FOLDER, exist_ok=True)
            
            # Get filename from URL
            filename = os.path.basename(parsed_url.path)
            
            # If filename is empty or invalid, create a unique filename
            if not filename:
                filename = f"downloaded_file_{int(time.time())}"
            
            # Full path for saving the file
            file_path = os.path.join(self.__TEMPORARY_FOLDER, filename)
            
            # Download file with timeout and stream to handle large files
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save file to disk
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        except requests.exceptions.RequestException as e:
            raise Exception(f"Error downloading file from {url}: {str(e)}")
        except IOError as e:
            raise Exception(f"Error saving file to {file_path}: {str(e)}")
    
    async def create_and_upload_zip(self):
            zip_path = f"{self.__TEMPORARY_FOLDER}/files.zip"
            with zipfile.ZipFile(zip_path, "w") as zipf:
                # List all files in the temporary folder
                for root, _, files in os.walk(self.__TEMPORARY_FOLDER):
                    for file in files:
                        # Skip the zip file itself to avoid recursion
                        if file == "files.zip":
                            continue
                        
                        file_path = os.path.join(root, file)
                        # Create relative path for the zip structure
                        arc_name = os.path.relpath(file_path, self.__TEMPORARY_FOLDER)
                        # Add file to the zip
                        zipf.write(file_path, arc_name)
            # Carica l'ZIP (usando PyGithub o Git)
            github = Github(self.__GITHUB_TOKEN)
            repo = github.get_repo(self.__GITHUB_REPO)
            with open(zip_path, "rb") as f:
                repo.create_file("downloads/files.zip", "Add batch", f.read(), "main")
            # Clean up temporary files after successful upload
            print("Cleaning up temporary files...")
            for root, dirs, files in os.walk(self.__TEMPORARY_FOLDER):
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Error deleting {file}: {str(e)}")
        
    async def entry_point(self) -> bool:
        """Metodo principale per l'esecuzione dello script"""
        data = json.loads(await self.get_json_data())
        print(f"Trovati {len(data['values'])} elementi")

        for idx, entry in enumerate(data['values']):
            try:
                await self.download_file(entry[0])
            except Exception as e:
                print(f"Errore durante il download del file. Indice {idx}: {str(e)}")
                pass
        await self.create_and_upload_zip()

        return True