import os.path
from datetime import datetime

#Récupération de la clé API
def cle_api_bot():
	with open('cleAPI.txt', 'r') as fichierCleAPI:
		cleAPI = fichierCleAPI.read()

	return cleAPI

#Convertit une date/heure UTC en date/heure locale
def date_utc_vers_local(date_utc):
	date_locale = date_utc.replace(tzinfo=timezone.utc).astimezone(tz=None)
	return date_local.replace(tzinfo=None)

#Permet de rechercher un utilisateur parmis ceux autorisés à dialoguer avec le bot
def utilisateur_autorise(utilisateurRecherche):
	nomFichierWhitelist = "whitelist.txt"

	#On récupère la liste des utilisateurs autorisés
	with open(nomFichierWhitelist,'r') as fichierWhitelist:
		listeUtilisateur = fichierWhitelist.readlines()

	#On supprime le '\n' au bout de chaque lignes
	for utilissateur in listeUtilisateur:
		utilisateur = utilisateur.rstrip()

	#S'il sagit de l'utilisateur recherché, on renvoie True
	if str(utilisateur) == str(utilisateurRecherche):
		return True

	#Sinon, on renvoie False
	return False