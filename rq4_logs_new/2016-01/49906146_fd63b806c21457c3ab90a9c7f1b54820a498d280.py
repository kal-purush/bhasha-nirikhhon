# -*-coding: utf8 -*
import soccersimulator
from soccersimulator.settings import *
from soccersimulator import BaseStrategy, SoccerAction
from soccersimulator import SoccerTeam, SoccerMatch
from soccersimulator import Vector2D, Player, SoccerTournament
import strategy

team1 = SoccerTeam("team1", [Player("t1j1", fonceurStrategy())])
team2 = SoccerTeam("team2", [Player("t2j1", fonceurStrategy())])
team3 = SoccerTeam("team3", [Player("t3j1", fonceurStrategy())])