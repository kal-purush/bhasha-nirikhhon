package main;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class BoardState implements Cloneable{
	int[][] pegPositions;
	int pathCost; 

	List<BoardState> children = new LinkedList<BoardState>();
	private int branchingFactor;

	public BoardState( int[][] pegPositions, int level )
	{
		this.pegPositions = pegPositions; 
		this.pathCost = pathCost; 
	}

	public int[][] getPegPositions() 
	{
		return pegPositions;
	}
	
	public void setBranchingFactor(int branchingFactor)
	{
		this.branchingFactor = branchingFactor;
	}

	public void setBoardState( int[][] newBoard, BoardState board) 
	{
 		board.pegPositions = newBoard; 
	}

	void setChildren( List<BoardState> children )
	{
		this.children = children; 
	}

	public List<BoardState> getChildren() 
	{
		return children;
	}
	protected Object clone() throws CloneNotSupportedException {
        return (BoardState) super.clone();
    }
}