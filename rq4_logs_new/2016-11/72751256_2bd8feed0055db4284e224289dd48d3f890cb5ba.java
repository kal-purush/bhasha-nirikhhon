package main;

public class MCHeuristics extends Heuristic
{
	public int getHeursticValue(MCBoardState board)
	{
		return board.getMissionaries() + board.getCannibals() - 1; 
	}
	
	public int getHeuristicCapacity(MCBoardState board)
	{
		return (board.getMissionaries() + board.getCannibals()) / board.getBoatCapacity(); 
	}
}