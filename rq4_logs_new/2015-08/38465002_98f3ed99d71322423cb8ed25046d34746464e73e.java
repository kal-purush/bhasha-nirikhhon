package maze.playerSet;

import maze.Position;

public class KnightMove implements DirectionSet{
	private static final Position NNE 	= new Position (-2,1);
	private static final Position NEE	= new Position (-1,2);;
	private static final Position SEE	= new Position (1,2);;
	private static final Position SSE	= new Position (2,1);;
	private static final Position SSW	= new Position (2,-1);
	private static final Position SWW	= new Position (1,-2);;
	private static final Position NWW	= new Position (-1,-2);;
	private static final Position NNW	= new Position (-2,-1);;

	private static final Position directionSet[] = {
			SEE,
			SSE,
			NEE,
			SSW,
			NNE,
			SWW,
			NNW,
			NWW
	};
	
	
	@Override
	public Position firstDirection() {
		return directionSet[0];
	}

	@Override
	public Position nextDirection(Position curDir) {
		for(int i = 0 ; i< directionSet.length-1; i++){ 
			//If final direction, can't reach here. 
			if(curDir.toString().equals( directionSet[i].toString() ))
				return directionSet[i+1];
		}
		return null;
	}

	@Override
	public boolean isFinalDirection(Position curDir) {
		if(NWW.toString().equals(curDir.toString())) return true;
		else return false;
	}

}