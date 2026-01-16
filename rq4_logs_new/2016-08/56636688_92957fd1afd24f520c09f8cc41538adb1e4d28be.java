package level2;

import java.util.*;

class DontPanic {

	public static void main(String args[]) {
		Scanner in = new Scanner(System.in);
		int nbFloors = in.nextInt(); // number of floors
		int width = in.nextInt(); // width of the area
		int nbRounds = in.nextInt(); // maximum number of rounds
		int exitFloor = in.nextInt(); // floor on which the exit is found
		int exitPos = in.nextInt(); // position of the exit on its floor
		int nbTotalClones = in.nextInt(); // number of generated clones
		int nbAdditionalElevators = in.nextInt(); // ignore (always zero)
		int nbElevators = in.nextInt(); // number of elevators

		// saving floors and elevators + exit floor
		int elevators[] = new int[nbFloors + 1];

		for (int i = 0; i < nbElevators; i++) {
			int elevatorFloor = in.nextInt(); // floor on which this elevator is
												// found
			int elevatorPos = in.nextInt(); // position of the elevator on its
											// floor
			elevators[elevatorFloor] = elevatorPos;
		}
		// adding exit floor to array
		elevators[exitFloor] = exitPos;

		System.err.println("1: " + elevators[0]);

		// game loop
		while (true) {
			int cloneFloor = in.nextInt(); // floor of the leading clone
			int clonePos = in.nextInt(); // position of the leading clone on its
											// floor
			String direction = in.next(); // direction of the leading clone:
											// LEFT or RIGHT

			// the case where there isn't leader clone; this may throw ArrayOut
			// of Bounds exception
			if (cloneFloor == -1 && clonePos == -1) {
				System.out.println("WAIT");
			}

			// if elevator is right and clone is heading right
			else if (direction.equals("RIGHT") && elevators[cloneFloor] >= clonePos) {
				System.out.println("WAIT");
			}

			// if elevator is left and clone is heading left
			else if (direction.equals("LEFT") && elevators[cloneFloor] <= clonePos) {
				System.out.println("WAIT");
			}

			// if elevator is left and clone is heading right
			else if (direction.equals("RIGHT") && elevators[cloneFloor] < clonePos) {
				System.out.println("BLOCK");
			}

			// if elevator is right and clone is heading left
			else if (direction.equals("LEFT") && elevators[cloneFloor] > clonePos) {
				System.out.println("BLOCK");
			}
		}
	}
}