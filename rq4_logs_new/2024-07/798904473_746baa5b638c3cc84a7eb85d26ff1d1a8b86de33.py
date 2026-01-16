import requests
from bs4 import BeautifulSoup
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pprint import pprint


# year = input("What year you would like to travel to? Type the date in this format YYY-MM-DD:  ")
URL = "https://www.billboard.com/charts/hot-100/2013-08-31/"

# Send GET request to the URL
response = requests.get(URL)
response.raise_for_status()  # Raise an exception for bad status codes
songs_webpage = response.text

# Parse the HTML content of the page
soup = BeautifulSoup(songs_webpage, "html.parser")

# Find all song title elements
song_names_spans = soup.select("li ul li h3")
song_names = [song.getText().strip() for song in song_names_spans]
print(song_names)


# Spotify authentication
SPOTIFY_CLIENT_ID = "1419683c96c44b58abf575082296ce4b"
SPOTIFY_CLIENT_SECRET = "858198fc115c4355b8890ccc8f1f3429"
REDIRECT_URI = "https://open.spotify.com/"
scope = "playlist-modify-private"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        redirect_uri=REDIRECT_URI,
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        # show_dialog=True,
        # cache_path="token.txt",
        username="Robert Chwedczuk"
    )
)

user_id = sp.current_user()["id"]


song_uris = []
year = "2013"  # Extract year from the URL

for song in song_names:
    try:
        result = sp.search(q=f"track:{song} year:{year}", type="track")
        uri = result["tracks"]["items"][0]["uri"]
        song_uris.append(uri)
    except IndexError:
        print(f"'{song}' not found on Spotify. Skipped.")
    except Exception as e:
        print(f"An error occurred while searching for '{song}': {str(e)}")

print(f"\nFound {len(song_uris)} song URIs:")
pprint(song_uris)