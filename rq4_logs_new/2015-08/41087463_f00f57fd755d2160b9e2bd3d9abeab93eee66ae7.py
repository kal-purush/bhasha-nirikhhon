#Eric Boniface MIA4
#Client graphique
from Tkinter import *
import string
import socket

#Création du socket de communication
s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#Fonction de réception des paquets
def recevoir(socket_r):
	retour_r=""
	#Tant que l'on ne recois pas un paquet contenant le signal de fin de transmission
	#on recoie les paquets et on les reassemble dans la chaine de caractere retour_r
	while (string.find(retour_r,"***FINENVOI***") == -1):
		print "while dans recevoir"
		retour_r = retour_r + socket_r.recv(10000)
		print retour_r
	print len(retour_r)
	#On retire le signal de fin de transmission de la chaine recus
	retour_r=string.replace(retour_r,"***FINENVOI***","")
	return retour_r	

#classe principale de l'interface graphique
class App:

	def __init__(self, master):
		
		#Fenetre principale de l'application
		self.frame = Frame(master)
		self.frame.pack()
	
		#Partie de la fenetre contenant les boutons
		frame2 = Frame(self.frame)
		frame2.pack(side=TOP)

		#Partie de la fenetre contenant les zones de textes
		frame3 = Frame(self.frame)
		frame3.pack(side=BOTTOM)

		#Bouton de fermeture de l'application
		self.fin = Button(frame2, text="Sortir", fg="red", command=self.com_sortie)
		self.fin.pack(side=LEFT)

		#Bouton de connection
		self.bconnect = Button(frame2, text="Connection", command=self.com_connect)
		self.bconnect.pack(side=LEFT)

		#Bouton pour lister le contenu d'un repertoire
		self.bdir = Button(frame2, text="Dir", command=self.com_dir)
		self.bdir.pack(side=LEFT)

		#Bouton pour afficher la version du systeme d'exploitation sur 
		#lequel on ext connecte
		self.bversion = Button(frame2, text="Version", command=self.com_version)
		self.bversion.pack(side=LEFT)

		#Bouton affichant le nom du repertoire courant
		self.brepcour = Button(frame2, text="Rep. Courant", command=self.com_repcour)
		self.brepcour.pack(side=LEFT)

		#Bouton ouvrant un fichier et l'affichant à l'ecran
		self.bouvrir = Button(frame2, text="Ouvrir fichier", command=self.com_ouvrir)
		self.bouvrir.pack(side=LEFT)
		
		#Bouton affcihant l'aide des commandes
		self.baide = Button(frame2, text="Aide", command=self.com_aide)
		self.baide.pack(side=LEFT)

		#Bouton pour se rendre dans le repertoire inferieur
		self.bcdd = Button(frame2, text="cd ..", command=self.com_cdd)
		self.bcdd.pack(side=LEFT)

		#Bouton pour se rendre dans un repertoire donne
		self.bcdr = Button(frame2, text="cd", command=self.com_cd)
		self.bcdr.pack(side=LEFT)

		#Zone d'affichage du repertoire courant
		self.rep=Entry(frame3)
		self.rep.pack(side=TOP,fill=X)

		#Zone de saisie
		self.repin=Entry(frame3)
		self.repin.pack(side=TOP,fill=X)

		#Barre de defilement pour la zone d'affichage principale
		self.scrolly = Scrollbar(frame3)
		self.scrolly.pack(side=RIGHT, fill=Y)

		self.scrollx = Scrollbar(frame3, orient=HORIZONTAL)
		self.scrollx.pack(side=BOTTOM, fill=X)

		#Zone d'affichage principale
		self.sortie = Text(frame3)
		self.sortie.pack(side=LEFT, fill=BOTH)

		self.scrolly.config(command=self.sortie.yview)
		self.scrollx.config(command=self.sortie.xview)

	def com_sortie(self):
		#methode pour sortir de l'application
		s.send("fin")
		self.frame.quit()

	def com_connect(self):
		#Methode pour la connection au serveur
		adresse=self.repin.get()
		s.connect((adresse,1236))
		self.com_dir()
		self.com_repcour() 

	def com_cd(self):
		#Methode pour changer de repertoire
		repertoire=self.repin.get()
		#envoie de la commande au serveur
		s.send("cd "+repertoire)
		retourc=""
		retour=recevoir(s)
		print retour
		self.sortie.insert(END,retour+"\n")
		#Affichage du repertoire courant apres le changement de repertoire
		self.com_repcour()
		#Liste le contenu du repertoire
		self.com_dir()
	
	def com_cdd(self):
		#methode pour decendre dans l'arborescence du serveur
		#envoie de la commande au serveur
		s.send("cd ..")
		retourc=""
		retour=recevoir(s)
		print retour
		self.sortie.insert(END,retour+"\n")
		#Affichage du repertoire courant apres le changement de repertoire
		self.com_repcour()
		#Liste le contenu du repertoire
		self.com_dir()

	def com_dir(self):
		#methode pour affciher la liste des fichiers du repertoire courant
		#envoie de la commande au serveur
		s.send("dir")
		retourc=""
		retour=recevoir(s)
		print retour
		self.sortie.delete(0.0,END)
		self.sortie.insert(END,retour+"\n")
		#Affichage du repertoire courant apres le changement de repertoire
		self.com_repcour()

	def com_ouvrir(self):
		#methode pour afficher le contenu d un fichier
		fichiero=self.repin.get()
		#envoie de la commande au serveur
		s.send("ouvrir "+fichiero)
		retourc=""
		retour=recevoir(s)
		print retour
		self.sortie.delete(0.0,END)
		self.sortie.insert(END,retour+"\n")
		#Affichage du repertoire courant apres le changement de repertoire
		self.com_repcour()
	    
	def com_version(self):
		#Methode pour afficher la version du systeme d'exploitation distant
		#envoie de la commande au serveur
		s.send("version")
		retour=recevoir(s)
		print retour
		self.sortie.delete(0.0,END)
		self.sortie.insert(END,retour+"\n")

	def com_aide(self):
		#Methode pour afficher l aide
		#envoie de la commande au serveur
		s.send("aide")
		retour=recevoir(s)
		print retour
		self.sortie.delete(0.0,END)
		self.sortie.insert(END,retour+"\n")

	def com_repcour(self):
		#Methode pour afficher le nom du repertoire courant
		#envoie de la commande au serveur
		s.send("repcour")
		retourc=""
		retour=recevoir(s)
		print retour
		self.rep.delete(0,END)
		self.rep.insert(END,retour)


#Creation de la fenetre principale
root = Tk()
#Appel de la classe contenant le fenetre principale
app = App(root)
#lancement de la boucle de gestion des evenements
root.mainloop()

