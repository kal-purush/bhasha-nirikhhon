package main;

import java.util.List;
import java.util.Vector;

public class MCBoardState extends BoardState
{

	public MCBoardState(int[] intlBoardState, int level) 
	{
		int[] initialBoardState = new int[] {3,3,1}; 
	
		//Initialize Game Board vector using user provided initialBoardState
		Vector<Integer> vector = new Vector<Integer>();
		for ( int i = 0; i < initialBoardState.length; i++ )
		{
			vector.add(initialBoardState[i]);
		}
		System.out.println(vector);
		
	}
	
	public List<BoardState> expand( MCBoardState board )
	{
		
		return children;
	}

}