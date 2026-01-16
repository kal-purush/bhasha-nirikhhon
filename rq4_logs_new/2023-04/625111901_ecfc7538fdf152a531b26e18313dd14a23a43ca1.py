
class Fen:

	def __init__(self, fen: str) -> None:
		self._fen = fen
		self._board = []
		ranks = fen.split('/')
		for rank in ranks:
			file = []
			for _, piece in enumerate(rank):
				if not piece.isdigit():
					file.append(piece)
				else:
					file += [None] * int(piece)
			self._board.append(file)


	def __getitem__(self, position):
		pass


fen = Fen('rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1')

print(list(reversed(fen._board)))
print(len(fen._board))