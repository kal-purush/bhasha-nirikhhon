package allocation;

import seebattle.fieldsandships.*;

// // pattern "strategy"

public class Locate {
	ShipsField shipsField;
	public Locate(ShipsField shF) {
		shipsField = shF;
	}
	public void locShip(ShipsPlaces shp, int i) { //pattern "strategy" 
		Ship ship = new Ship();
		for (Coordinate c : shp.placeForShip()) {  
			ship.addToBlocks(c);		
		}
		ship.setMotors(i);
		// mark ship environment
		shipsField.setShipsEnvironmentMarkers(ship);
		shipsField.addToListShips(new Ship(ship.getBlocks()));
		System.out.println(ship.toString());
	}
}