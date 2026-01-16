from bs4 import BeautifulSoup
import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pprint import pprint


BILLBOARD_URL = "https://www.billboard.com/charts/hot-100/"
CLIENT_ID = "ab85b014d9fe44bbac9a6dd8a2601deb"
CLIENT_SECRET = "c3ebe9cfa7954b57b4989116dcec51be"

date = input("Which year would you like to travel back to? Type the date in this format YYYY-MM-DD: ")
response = requests.get(f"https://www.billboard.com/charts/hot-100/{date}")
top_hundred = response.text

soup = BeautifulSoup(top_hundred, "html.parser")
songs = soup.find_all(name="span", class_="chart-element__information__song")
song_list = [song.getText() for song in songs]


sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri="http://example.com",
        scope="playlist-modify-private",
    )
)
user_id = sp.current_user()['id']

song_uris = []
year = date.split("-")[0]
for song in song_list:
    result = sp.search(q=f"track: {song} year: {year}", type="track")
    try:
        uri = result["tracks"]["items"][0]["uri"]
        song_uris.append(uri)
    except IndexError:
        print(f"{song} does not exist in spotify. Song skipped.")

new_playlist = sp.user_playlist_create(user=user_id, name=f"{date} Billboard 100", public=False)
sp.user_playlist_add_tracks(user=user_id, playlist_id=new_playlist['id'], tracks=song_uris)