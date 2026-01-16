#!/usr/bin/python3

import sys
from sqlobject_minefield import *
from game import *
from render import *
import pygame as PG


def at_exit(game):
	print(game.board.id)
	quit()

#main loop

if len(sys.argv) > 1:
	game = Game(board = int(sys.argv[1]))
else:
	print ("Uso: \"./repetition <board_id>\"")

render = Render(game.board.chunk_size)


actions = Action.select(SO.AND(Chunk.q.board==game.board, Action.j.chunk), orderBy = Action.q.id)
for action in actions:
	while action.chunk.i != game.current_i or action.chunk.k != game.current_k:
		if action.chunk.i < game.current_i:
			game.change_chunk('LEFT')
		elif action.chunk.i > game.current_i:
			game.change_chunk('RIGHT')
		elif action.chunk.k < game.current_k:
			game.change_chunk('UP')
		elif action.chunk.k > game.current_k:
			game.change_chunk('DOWN')

		render.render_screen( game.get_displays(action.id-1) )			
		time.sleep(0.5)	

	render.render_screen( game.get_displays(action.id) )			
	for event in PG.event.get():
		if event.type == PG.QUIT:
			at_exit(game)
	
		if event.type == PG.KEYDOWN:
			if event.key == PG.K_ESCAPE:
				at_exit(game)
	
	time.sleep(0.1)