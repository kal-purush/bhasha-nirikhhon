package game.interf;

import game.Game;
import game.Player;
import pieces.Piece;

public interface GameUInterface {

	void changeTurn(Player player);
	boolean changeTheIcons(int row,int colum,Piece piece);
	void showColorMoves(int row,int colum);
	boolean isSameColor(Game game,int row,int colum);
}