# EDM5240
# coding : utf-8 
#la ligne du haut est pour activer mon python 

#je me prépare pour la moisson de mes données en plaçant mes éléemnts pour créer un dossier csv et ouvrir BeautifulSoup
import csv 
import requests 
from bs4 import BeautifulSoup 
#J'entre dans mon "bash" les éléments pour activer BeautifulSoup 

#J'inscris l'URL du ministère de la santé que je vais utiliser 
url= "http://www.contracts-contrats.phac-aspc.gc.ca/phac-aspc/pd-dp/cd-dc.nsf/WEBbypurposeF?OpenView&RP=2016-2017~1&Count=3000&lang=fra&"

#je me créer un fichier et un nom de fichier pour mon dossier csv qui prendra mes données en direct du site internet 
fichier = "contrats-sante.csv"

#Par politesse, j'écris qui je suis, ce que je veux et mon email en cas de besoin, ça ne cahngera pas mon script, c'est pour prévenir
entetes = {
    "User-Agent":"Beatrice Roy-Brunet-Pour une emission",
    "From":"beatriceroybrunet@gmail.com"
}

#je fais ma connexion avec mon URL et je place mes données dans "contenu"
contenu = requests.get(url,headers=entetes)

#BeautifulSoup va analyser ces données et les mettre dans page (le bon mot est parser)
page= BeautifulSoup(contenu.text,"html.parser")
#je print pour voir le résultat dans mon bash 
print(page)

#je ne veux pas les entêtes du tableau 
i=0 

#Je créer une boucle pour analyser toutes les cases de mon tableau 
for ligne in page.find_all("tr"):
    if i !=0: 
        
        print(ligne)
        lien = ligne.a.get("href")
        print(lien)
        #je veux mon hyperlien 
        lien = "http://www.contracts-contrats.phac-aspc.gc.ca/phac-aspc/pd-dp/cd-dc.nsf/"+ lien 
        print(lien)
   
   #je vais maintenant chercher les infos relatives aux contrats du tableau 
        contenu2 = requests.get(lien,headers=entetes)
        page2 = BeautifulSoup(contenu2.text,"html.parser")
    #sante sera ma liste vide pour placer mes résultats 
        sante = []
    
        sante.append(lien)
    #pour aller chercher chaque élément de typre tr de mon tableau 
        for item in page2.find_all("tr"):
             print(item)
        #si une des cases est vide je rajoute ceci, pourtant ça faisait planter mon programme, je l'ai mis en # et tout fonctionne maintenant
            #if item.td is not None:
                #sante.append(item.td.text)
            #else:
                #santee.append(None)
    
        print(sante)
    
    #je créer un dossier csv qui va s'appeler contrats-sante.csv 
        pomme = open(fichier,"a")
        ministere = csv.writer(pomme)
        ministere.writerow(sante)
        
    #j'ajoute 1     
    i += 1