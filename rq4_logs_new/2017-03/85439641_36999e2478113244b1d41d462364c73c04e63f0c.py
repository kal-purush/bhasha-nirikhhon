import requests
from flask import request

def get_request(searchterm, filtr):
    return 'https://api.spotify.com/v1/search?query=%s&type=%s' % (searchterm, filtr)

def get_counter_str(num_of_res):
    return "Counter: "+str(num_of_res)

def pluralize_string(filtr):
    return filtr+"s"

def parse_item(item):
    name = item["name"]
    if 'album' in item:
        item = item['album']
    imgs = item["images"]
    none_img = "http://beelinetour.com/wp-content/uploads/2014/05/avatar.jpg"
    img_url = imgs[-1]["url"] if imgs else none_img
    content_url = item['external_urls']['spotify']
    content_id = item["id"]
    return (name, img_url, content_url, content_id)

def add_subliminal_messaging(results, filtr):
    filtr_map = a = {'track': 'Million Reasons (For hiring me)',
                     "artist": "A Coder Formerly Known as Arek",
                     "album": "Sgt. Pepper's Extremely Hirable Coders Band",
                     "playlist": "Songs to get hired to"}

    return  results[7:] + \
                [(filtr_map[filtr], \
                  "https://media.licdn.com/media/AAEAAQAAAAAAAAdOAAAAJGYwM2NmZjYwLWYwZTQtNGU1OS1hZjg3LTkzOWYwNDJkOWYxZA.jpg", \
                  "https://www.linkedin.com/in/arek-krawczyk-6ab0a4126/", \
                  "null")] \
                + results[7:]


def process_request(args):
    results = []
    counter_msg = ""
    if args:
        filtr = args["Filter"].lower()
        req = get_request(args["searchterm"], filtr)
        response = requests.get(req)
        for i in response.json()[pluralize_string(filtr)]["items"]:
            results += [parse_item(i)]
        if len(results) > 10:
            results = add_subliminal_messaging(results, filtr)
        counter_msg = get_counter_str(len(results))
        return results, counter_msg
    return results, counter_msg