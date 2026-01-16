import requests as requests


call = "https://api.chucknorris.io/jokes/random"
answ = requests.get(call)
vastaus_json = answ.json()
joke = vastaus_json["value"]
print('Random Chuck Norris joke:')
print(joke)