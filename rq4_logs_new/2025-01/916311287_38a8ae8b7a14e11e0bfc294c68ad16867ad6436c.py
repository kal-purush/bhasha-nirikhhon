import requests
import os
from dotenv import load_dotenv

load_dotenv()

URL = "https://api.api-ninjas.com/v1/animals"
API_KEY_NAME = "X-Api-Key"
API_KEY = os.getenv('API_KEY')
print(API_KEY)

def fetch_data(animal_name):
  """
  Fetches the animals data for the animal 'animal_name'.
  Returns: a list of animals, each animal is a dictionary:
  """
  req_url = f"{URL}?name={animal_name}&{API_KEY_NAME}={API_KEY}"
  res = requests.get(req_url)
  animal_data = res.json()
  return animal_data