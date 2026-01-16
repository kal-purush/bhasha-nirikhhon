from soccersimulator import Strategy, SoccerAction, Vector2D
from soccersimulator import SoccerTeam, Simulation
from soccersimulator import show_simu
PLAYER_RADIUS = 1. # Rayon d un joueur
BALL_RADIUS = 0.65 # Rayon de la balle

## Fonceur sur la balle
class Fonceur(Strategy):
    def __init__(self):
        Strategy.__init__(self,"Random")
    def compute_strategy(self,state,id_team,id_player):
    	if ((state.ball.position - state.player_state(id_team,id_player).position).norm < PLAYER_RADIUS + BALL_RADIUS):
    		if (id_team == 1):
    			if (state.ball.position.x > 112):
    				return SoccerAction(state.ball.position - state.player_state(id_team,id_player).position ,(Vector2D(150,45) - state.ball.position)/4.5)
    			return SoccerAction(state.ball.position - state.player_state(id_team,id_player).position ,(Vector2D(150,45) - state.ball.position)/15)
    		if (id_team == 2):
    			if (state.ball.position.x < 37):
    				return SoccerAction(state.ball.position - state.player_state(id_team,id_player).position ,(Vector2D(0,45) - state.ball.position)/4.5)
    			return SoccerAction(state.ball.position - state.player_state(id_team,id_player).position ,(Vector2D(0,45) - state.ball.position)/15)
    	else:
    		return SoccerAction(state.ball.position - state.player_state(id_team,id_player).position ,0)
    			

## Strategie aleatoire
class RandomStrategy(Strategy):
    def __init__(self):
        Strategy.__init__(self,"Random")
    def compute_strategy(self,state,id_team,id_player):
        return SoccerAction(Vector2D.create_random(-200,200),0)
    		
    		
    		
    		
    		
    		
    		
    		