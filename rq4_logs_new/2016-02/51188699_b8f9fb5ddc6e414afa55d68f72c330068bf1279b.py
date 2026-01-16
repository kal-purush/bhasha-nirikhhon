import urllib2

api_key = "\"your-api-key\""
querry_base = '{"api_key" : ' + api_key +',"fields" : [ "name", "location", "contact" ],'
url = 'https://api.locu.com/v2/venue/search'

def make_request(json_querry):
    req = urllib2.Request(url, json_querry, {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)
    for x in f:
        print(x)
    f.close()

def querry_venue_by_name(venue_name):
    json_querry = querry_base + '"venue_queries" : [{"name" : \"' + venue_name + '\"}]}'
    return json_querry

# def querry_locality_menu_item(locality, menu_item):
#     json_querry = querry_base + '"venue_queries" : [{"location" : {"locality": "' + locality +'"}}], "menu_item_queries" : [{"name" : "pizza"}]}'
#     return json_querry

def querry_locality(locality):
    json_querry = querry_base + '"venue_queries" : [{"location" : {"locality": "' + locality +'"}}]}'
    return json_querry


q = querry_venue_by_name("Bistro Central Parc")
print q
make_request(q)
q = querry_locality("Boston")
print q
make_request(q)
# q = querry_locality_menu_item("Boston", "pizza")
# print q
# make_request(q)