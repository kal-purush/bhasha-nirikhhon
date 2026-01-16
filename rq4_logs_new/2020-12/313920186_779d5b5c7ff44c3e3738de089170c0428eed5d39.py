#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
# Authors: Julien MARTIN-PRIN & Valentin FERNANDES

from objects.tournament import *
from objects.map import *
from objects.game import *
from objects.player import *

def main():
	"""
	Let's play the game ! 
	"""
	player0=Player(0,"Zerator")
	player1=Player(1,"Ponce")
	player2=Player(2,"Antoine Daniel")
	player3=Player(3,"Rincevent")
	player4=Player(4,"Deuxfleur")
	player5=Player(5,"LBW")
	player6=Player(6,"Etoile")
	player7=Player(7,"Alpha")
	player8=Player(8,"Flora")
	player9=Player(9,"Hrun")
	List_player = [player1,player2,player3,player4,player5,player6,player7,player8,player9]
	AmongUs = Game(1,List_player)

	print("Do you want play the game with vote ? (y/n)")
	print("if no, the vote will be generated randomly. Else you have to vote and unmask the terrible impostors")
	decision = input('> ')	
	if decision=='y':		
		AmongUs.Start()
	else : 
		AmongUs.Start(RndVote =False)

if __name__ == '__main__':
	main()