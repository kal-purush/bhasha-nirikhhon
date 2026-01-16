import urllib.request
import os
from hashlib import md5
from hashlib import sha256
import zipfile
import socket
import re
import json


def problema1(s):
	return  re.findall("[02468]+",s)

def problema2(url,cheie):
	data = urllib.request.urlopen(url).read()
	details = json.loads(data)
	return (details.get(cheie, [])[-1:])[0]

def problema3(path):
	hashes = {}
	for ents in os.walk(path):
		value = md5(ents[0].encode()).hexdigest()
		for fname in ents[2]:
			hashes[fname] = value
	return hashes

def problema4(lista_arhive):
	list = []
	for i in lista_arhive:
		zp = zipfile.ZipFile(i)
		for zip_file in zp.filelist:
			if zip_file.filename not in list:
				list.append(zip_file.filename)
	return list

def problema5(url):
	data = urllib.request.urlopen(url).read()
	details = json.loads(data)
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host, port, info = [details[key] for key in ("ip", "port", "info")]
	sock.connect((host, port))
	sock.send(info.encode())
	resp = sock.recv(10)
	obtained = resp.decode()
	return obtained.count("A")




#print(problema1("azi, 22ianuarie2019 dau testul 2 la python3.xx"))
#print(problema2("file:///C:/Users/Milea%20Mihai/Desktop/ceva.json","key1"))
#print(problema3("."))
#print(problema5("http://fiieval.tk/static/testdata/14cedf72-c41c-40d1-b6c8-7ec6a6d9fd19.json"))
#print(problema5("file:///C:/Users/Milea%20Mihai/Desktop/ceva.json"))
#print(problema4(['hai/behind.zip','hai/arch.zip']))