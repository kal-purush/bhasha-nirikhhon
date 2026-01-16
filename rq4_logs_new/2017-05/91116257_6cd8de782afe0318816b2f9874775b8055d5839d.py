import urllib.request
import xmltodict
import backend.utils.db as utils

from flask import render_template
from flask import redirect

API_KEY = "6be54ea8ecd35448b04f9d29183d0138"

def artist(request, session, artist_name = None, album_name = None):
	print ("INIZIO" + artist_name)
	top_albums = [None, None, None, None]

	artist_xml = "http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={}&api_key={}".format(artist_name, API_KEY)
	top_album_xml = "http://ws.audioscrobbler.com/2.0/?method=artist.gettopalbums&artist={}&api_key={}".format(artist_name, API_KEY)
	
	xml = urllib.request.urlopen(artist_xml)
	xml_top_album = urllib.request.urlopen(top_album_xml)

	xml_dict = xmltodict.parse(xml)
	xml_dict_top = xmltodict.parse(xml_top_album)
	

	artist_info = xml_dict['lfm']['artist']['bio']['content']

	for i in range(0,4):
		print(artist_name)
		print(xml_dict_top['lfm']['topalbums']['album'][i]['name'])
		top_albums[i]= artist_name + "_" + xml_dict_top['lfm']['topalbums']['album'][i]['name']

	print (top_albums)
	return render_template("artist.html", artist_name = artist_name, artist_info = artist_info, imm = top_albums)