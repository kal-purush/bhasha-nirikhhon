#!/usr/bin/env python

def meta_scrape():
	## Meta-Data Representing Knowledge before the Match
	## To be scraped dynamically from source, e.g.:
	## http://www.flashscore.com/match/WnVNPAX3/#lineups;1
	## Following is hard-coded for now.
	team_data = ["Arsenal","Hull_City"]
	player_data = [("Bellerin_H.","Arsenal"),("Cazorla_S.","Arsenal"),("Coquelin_F.","Arsenal"),("Giroud_O.","Arsenal"),("Koscielny_L.","Arsenal"),("Mertesacker_P.","Arsenal"),("Monreal_N.","Arsenal"),("Ospina_D.","Arsenal"),("Ozil_M.","Arsenal"),("Ramsey_A.","Arsenal"),("Sanchez_A.","Arsenal"),('Aluko_S.','Hull_City'),('Brady_R.','Hull_City'),('Chester_J.','Hull_City'),('Dawson_M.','Hull_City'),('Elmohamady_A.','Hull_City'),('Harper_S.','Hull_City'),('Huddlestone_T.','Hull_City'),('Livermore_J.','Hull_City'),('McShane_P.','Hull_City'),('N\'Doye_D.','Hull_City'),('Quinn_S.','Hull_City')]
	bench_data = [('Flamini_M.','Arsenal'),('Gibbs_K.','Arsenal'),('Gabriel','Arsenal'),('Rosicky_T.','Arsenal'),('Szczesny_W.')('Walcott_T.','Arsenal'),('Wilshere_J.','Arsenal'),('Bruce_A.','Hull_City'),('Hernandez_A.','Hull_City'),('Jelavic_N.','Hull_City'),('McGregor_A.','Hull_City'),('Meyler_D.','Hull_City'),('Robertson_A.','Hull_City'),('Rosenior_L.','Hull_City')]

	return team_data
	return player_data
	return bench_data