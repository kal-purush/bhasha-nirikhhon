package main;

import java.util.List;

public class GreedyBestFirstSearch extends Search 
{

	@Override
	public boolean find(BoardState initialState, BoardState goalState) 
	{
		boolean found = false; 

		//		@Override
		//		public String toString() {
		//			return "Greedy Best First Search";
		//		}

		fringe.add(initialState);
		System.out.println(fringe);

		//BoardState previous = initialState;

		while (!found && fringe.size() != 0 ) 
		{
			BoardState best = fringe.get(0);
			for( int i = 0; i < fringe.size(); i++ )
			{
				if (fringe.get(i).getHeuristicCost(fringe.get(i)) + pathCost < best.getHeuristicCost(best)) 
				{
					best = fringe.get(i);
				}
			}
			//System.out.println("hesre");
			if (!checkGoalState(best, goalState, pegsRemaining) )
			{
				if(!closed.contains(best))
				{
					expand(best);
					closed.add(best);

					//best.setPathCost(best.getPathCost() + 1);
					for( int i = 0; i < best.children.size(); i++ )
					{
						fringe.add(best.children.get(i));
					}			
				}
				nodesExamined++;
			}
			else
			{
				System.out.println("FOUND");
				Driver.printArray(best);
				found = true; 
			}
			
			System.out.println("Still searching... " + nodesExamined + " nodes examined.");
		}
		return found;
	}

}