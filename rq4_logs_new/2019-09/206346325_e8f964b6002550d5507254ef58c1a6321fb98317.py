import requests
from flask import Flask, redirect, render_template, request

app = Flask(__name__)


# set global variables

####### Insert Client Id here #########
client_id= ""
####### Insert Client Secret here #########
client_secret= ""
header_info = {}
genres=[]

# class for each recommendation
class getTrack:
        def __init__(self, track):
                self.track = track
        def get_track_name(self, *args):
                return self.track["name"]
        def get_artist(self, **kwargs):
                for items in self.track["artists"]:
                        return items["name"]
        def get_image_url(self, *args):
                album = self.track["album"]
                for image in album["images"]:
                        return image["url"]
        def get_link(self):
                url = self.track["external_urls"]
                return url["spotify"]


# redirect route url to get Spotify code
@app.route("/")
def getAuth():
        return redirect(f"https://accounts.spotify.com/authorize?client_id={client_id}&response_type=code&redirect_uri=http://localhost:5000/home&", code=302)

# redirect page after accepting auth
@app.route("/home")
def welcome():
        global header_info
        global genres
        code = request.args.get('code', '')
        body = {'grant_type':'authorization_code', 
        'code':code, 
        'redirect_uri':'http://localhost:5000/home',
        'client_id':client_id,
        'client_secret':client_secret} 
        info = requests.post("https://accounts.spotify.com/api/token", data = body).json()
        header_info = {
       'Authorization': 'Bearer ' + info['access_token']
        }
        # get available seed genres and use them to populate dropdown menus
        categories = requests.get("https://api.spotify.com/v1/recommendations/available-genre-seeds", headers = header_info).json()
        genres = categories["genres"]
        return render_template("home.html", categories=genres)

# results path        
@app.route("/results", methods=['POST'])
def results():
        # use the form data in the GET request
        genre = f"{request.form['genres1']}, {request.form['genres2']}, {request.form['genres3']}"
        dance = int(request.form['danceability'])/10
        energy = int(request.form['energy'])/10
        valence = int(request.form['valence'])/10
        info = requests.get(f"https://api.spotify.com/v1/recommendations?seed_genres={genre}&limit=9&max_danceability={dance}&max_energy={energy}&max_valence={valence}&max_popularity=60", headers = header_info).json()
        final = []
        # create a list of dictionaries
        for track in info["tracks"]:
                #set an empty dictionary for each recommended track
                singular= {}
                data = getTrack(track)
                # get track name, add it to the dictionary
                name = data.get_track_name()
                singular["name"] = name
                # get artist name, add it to the dictionary
                artist = data.get_artist()
                singular["artist"] = artist
                # add the image url to the dictionary
                image = data.get_image_url()
                singular["image"] = image
                #add the dictionary to the list of final information
                url = data.get_link()
                singular["url"] = url
                final.append(singular)
        return render_template("results.html", recommendations=final, categories=genres)
if __name__ == "__main__":
    app.run(debug=True)