package assignment4;
/* CRITTERS Critter.java
 * EE422C Project 4 submission by
 *
 * Elvin J. Galarza
 * ejg2298
 * 15455
 *
 * Bianca Antonio
 * bla774
 * 15510
 *
 * Slip days used: <0>
 * Spring 2018
 *
 */


import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/* see the PDF for descriptions of the methods and fields in this class
 * you may add fields, methods or inner classes to Critter ONLY if you make your additions private
 * no new public, protected or default-package code or data can be added to Critter
 */


/**
 * Maintains the world's operations and can display it.
 * All objects extend from this abstract class.
 */
public abstract class Critter {
	private static String myPackage;
	private	static List<Critter> population = new java.util.ArrayList<Critter>();
	private static List<Critter> babies = new java.util.ArrayList<Critter>();
	// Gets the package name.  This assumes that Critter and its subclasses are all in the same package.
	static {
		myPackage = Critter.class.getPackage().toString().split(" ")[1];
	}
	private static java.util.Random rand = new java.util.Random();
	/**
	 * Generates a random int from 0 to max - 1
	 * @param max from setSeed
	 * @return An int from 0 to max-1
	 */
	public static int getRandomInt(int max) {
		return rand.nextInt(max);
	}
	/**
	 * Sets the seed needed for the random number generatro that is used
	 * in getRandomInt
	 * @param new_seed The new seed number for the random number generator
	 */
	public static void setSeed(long new_seed) {
		rand = new java.util.Random(new_seed);
	}
	/* a one-character long string that visually depicts your critter in the ASCII interface */
	public String toString() {
		return "";
	}





	private int energy = 0;
	protected int getEnergy() {
		return energy;
	}



	private int x_coord;
	private int y_coord;
	/* my variables */
	private static int time_step = 0; /* time step counter */
	private boolean moved; /* tells us if the Critter has moved (true if so) during this time step */

	/**
	 * This function moves the Critter in a given direction
	 * and steps. Keep in mind that the map is numbered like
	 * a matrix ( think: Battleship gameboard). So we start
	 * from (0,0) and increase in x as we go right and increase
	 * in y as we go down. Map is a torus (i.e., wraps around like
	 * Pacman)
	 * @param direction direction the Critter moves in
	 *                  Think of a circle and the int represents the radians
	 *                  0 = straight RIGHT(increasing x, no change in y)
	 *                  1 = diagonally UP and to the RIGHT(increasing x, decreasing y)
	 *                  2 = straight UP (unchanged x, decreasing y)
	 *                  3 = diagonally UP and to the LEFT(decreasing x, decreasing y)
	 *                  4 = straight LEFT (decreasing x, unchanged y)
	 *                  5 = diagonally DOWN and to the LEFT (decreasing x, increasing y)
	 *                  6 = straight DOWN (unchanged x, increasing y)
	 *                  7 = diagonally DOWN and to the RIGHT (increasing x, increasing y)
	 * @param steps amount of steps the Critter takes
	 */
	private final void move(int direction, int steps){
		int newX = x_coord;
		int newY = y_coord;
		/* debugging
		System.out.println("Direction: " + direction);
		System.out.println("Old Coordinate: " + newX + " " + newY); */

		switch(direction){

			/* right */
			case 0:
				newX += steps;
				if(newX >= Params.world_width){
					newX = wrapX(newX);
				}
				break;

			/* right-up diagonal */
			case 1:
				newX += steps;
				newY -= steps;
				if(newX >= Params.world_width){
					newX = wrapX(newX);
				}
				if(newY < 0){
					newY = wrapY(newY);
				}
				break;

			/* up */
			case 2:
				newY -= steps;
				if(newY < 0){
					newY = wrapY(newY);
				}
				break;

			/* left-up diagonal */
			case 3:
				newX -= steps;
				newY -= steps;
				if(newX < 0){
					newX = wrapX(newX);
				}
				if(newY < 0){
					newY = wrapY(newY);
				}
				break;

			/* left */
			case 4:
				newX -= steps;
				if(newX < 0){
					newX = wrapX(newX);
				}
				break;

			/* down-left diagonal */
			case 5:
				newX -= steps;
				newY += steps;

				if(newX < 0){
					newX = wrapX(newX);
				}
				if(newY >= Params.world_height){
					newY = wrapY(newY);
				}
				break;

			/* down */
			case 6:
				newY += steps;
				if(newY >= Params.world_height){
					newY = wrapY(newY);
				}
				break;

			/* down-right diagonal */
			case 7:
				newX += steps;
				newY += steps;

				if(newX >= Params.world_width){
					newX = wrapX(newX);
				}
				if(newY >= Params.world_height){
					newY = wrapY(newY);
				}
				break;

			default:
				break;
		}

		x_coord = newX;
		y_coord = newY;

		/* debugging
		System.out.println("New Coordinate: " + x_coord + " " + y_coord); */
	}
	/* helper functions for move method that wraps
	   the Critter around the map (TORUS)*/
	private int wrapX(int x){
		return ((x % Params.world_width + Params.world_width) % Params.world_width);
	}
	private int wrapY(int y){
		return ((y % Params.world_height + Params.world_height) % Params.world_height);
	}



	/**
	 * Simulates a Critter's walk operation.
	 * Uses specified direction to move a tile.
	 * Reduces energy from Critter.
	 * @param direction int that represents a certain direction
	 */
	protected final void walk(int direction) {
		if(moved == false){
			move(direction, 1);
			moved = true;
		}
		energy = energy - Params.walk_energy_cost;
	}

	/**
	 * Simulates a Critter's run operation.
	 * Uses specific direction to move two tiles.
	 * Reduces energy from Critter.
	 * @param direction int that represents a certain direction
	 */
	protected final void run(int direction) {
		if(moved == false){
			move(direction, 2);
			moved = true;
		}
		energy = energy - Params.run_energy_cost;
	}

	/**
	 * Simulates reproducing Critters.
	 * Creates a new Critter object by expending parent's energy
	 * if there is enough energy to use. Child gets half of parent's
	 * original energy (rounded down) and parent is obviously left with
	 * the remaining half (rounded up). Child is placed adjacent to
	 * parent (specified by direction), and is encounter-immune. Babies
	 * are not added to general population until after timestep is finished.
	 * So technically the baby is not existing yet, but is prepped.
	 * @param offspring Critter object (offspring)
	 * @param direction int that represents a certain direction
	 */
	protected final void reproduce(Critter offspring, int direction) {
		if(this.energy >= Params.min_reproduce_energy){
			offspring.energy = this.energy/2; /* give offspring energy */
			this.energy -= offspring.energy; /* give parent modified energy */

			/* place offspring adjacent to parent */
			offspring.x_coord = this.x_coord;
			offspring.y_coord = this.y_coord;
			offspring.move(direction, 1);

			/* add offspring to babies */
			babies.add(offspring);
		}
	}

	public abstract void doTimeStep();
	public abstract boolean fight(String oponent);



	/* CRITTER COLLECTION: STAGE ONE */
	/**
	 * create and initialize a Critter subclass.
	 * critter_class_name must be the unqualified name of a concrete subclass of Critter, if not,
	 * an InvalidCritterException must be thrown.
	 * (Java weirdness: Exception throwing does not work properly if the parameter has lower-case instead of
	 * upper. For example, if craig is supplied instead of Craig, an error is thrown instead of
	 * an Exception.)
	 * @param critter_class_name name of the Critter object
	 * @throws InvalidCritterException thrown error
	 */
	public static void makeCritter(String critter_class_name) throws InvalidCritterException {

		try{
			/* check input first */
			Critter newCritter = (Critter) Class.forName(myPackage + "." + critter_class_name).newInstance();
			/* add  the newCritter to the world's population */
			Critter.population.add(newCritter);
			/* give the newCritter its starting energy */
			newCritter.energy = Params.start_energy;
			/* give the newCritter a random start position in the world */
			newCritter.x_coord = Critter.getRandomInt(Params.world_width);
			newCritter.y_coord = Critter.getRandomInt(Params.world_height);
		}
		catch(ClassNotFoundException|InstantiationException|IllegalAccessException exception){
			throw new InvalidCritterException(critter_class_name);
		}
	}
	/**
	 * Gets a list of critters of a specific type.
	 * @param critter_class_name What kind of Critter is to be listed.  Unqualified class name.
	 * @return List of Critters.
	 * @throws InvalidCritterException thrown error
	 */
	public static List<Critter> getInstances(String critter_class_name) throws InvalidCritterException {
		List<Critter> result = new java.util.ArrayList<Critter>();

		Class<? extends Critter> get_list = null;

		try{
			get_list = Class.forName(myPackage + "." + critter_class_name).asSubclass(Critter.class);
		}
		catch(ClassNotFoundException critter){
			throw new InvalidCritterException(critter_class_name);
		}

		for(Critter critter: population){
			if(get_list.isInstance(critter)){
				result.add(critter);
			}
		}

		return result;
	}

	/**
	 * Prints out how many Critters of each type there are on the board.
	 * @param critters List of Critters.
	 */
	public static void runStats(List<Critter> critters) {
		System.out.print("" + critters.size() + " critters as follows -- ");
		java.util.Map<String, Integer> critter_count = new java.util.HashMap<String, Integer>();
		for (Critter crit : critters) {
			String crit_string = crit.toString();
			Integer old_count = critter_count.get(crit_string);
			if (old_count == null) {
				critter_count.put(crit_string,  1);
			} else {
				critter_count.put(crit_string, old_count.intValue() + 1);
			}
		}
		String prefix = "";
		for (String s : critter_count.keySet()) {
			System.out.print(prefix + s + ":" + critter_count.get(s));
			prefix = ", ";
		}
		System.out.println();
	}

	/* the TestCritter class allows some critters to "cheat". If you want to
	 * create tests of your Critter model, you can create subclasses of this class
	 * and then use the setter functions contained here.
	 *
	 * NOTE: you must make sure that the setter functions work with your implementation
	 * of Critter. That means, if you're recording the positions of your critters
	 * using some sort of external grid or some other data structure in addition
	 * to the x_coord and y_coord functions, then you MUST update these setter functions
	 * so that they correctly update your grid/data structure.
	 */
	static abstract class TestCritter extends Critter {
		protected void setEnergy(int new_energy_value) {
			super.energy = new_energy_value;
		}

		protected void setX_coord(int new_x_coord) {
			super.x_coord = new_x_coord;
		}

		protected void setY_coord(int new_y_coord) {
			super.y_coord = new_y_coord;
		}

		protected int getX_coord() {
			return super.x_coord;
		}

		protected int getY_coord() {
			return super.y_coord;
		}


		/*
		 * This method getPopulation has to be modified by you if you are not using the population
		 * ArrayList that has been provided in the starter code.  In any case, it has to be
		 * implemented for grading tests to work.
		 */
		protected static List<Critter> getPopulation() {
			return population;
		}

		/*
		 * This method getBabies has to be modified by you if you are not using the babies
		 * ArrayList that has been provided in the starter code.  In any case, it has to be
		 * implemented for grading tests to work.  Babies should be added to the general population
		 * at either the beginning OR the end of every timestep.
		 */
		protected static List<Critter> getBabies() {
			return babies;
		}
	}

	/**
	 * Clear the world of all critters, dead and alive
	 */
	public static void clearWorld() {
		population.clear();
		babies.clear();
	}

	/* Time Steps: STAGE 1 */
	/**
	 * Simulation consists of a sequence of time steps. During
	 * each step, the state of all Critters in simulation is
	 * updated, new critters may be added, and critters may be
	 * removed (births and deaths). All core functionality is
	 * associated with time steps.
	 *
	 * Simulates one time step for every Critter in the
	 *   critter collection (i.e., entire world).
	 */
	public static void worldTimeStep() {
		/*
		 * 1. increment timestep; timestep++;
		 * 2. doTimeSteps();
		 * 3. Do the fights. doEncounters();
		 * 4. updateRestEnergy();
		 * 5. Generate Algae genAlgae();
		 * 6. Move babies to general population. population.addAll(babies); babies.clear();
		 *
		 */

		/* 1 */
		time_step++;

		/* 2) */
		doTimeSteps();

		/* 3)
		 * First, find fights (encounter).
		 * Then, simulate fighting.
		 */
		doEncounters();

		/* 4*/
		/* checks Critter: allows for removing death critters, energy upkeep, enables Critter to move again */
		updateRestEnergy();

		/* 5 */
		genAlgae();

		/* 6 */
		population.addAll(babies);
		babies.clear();
	}

	/**
	 * does the time step for each Critter alive
	 */
	private static void doTimeSteps(){
		for(Critter critter : population){
			critter.doTimeStep();
		}
	}

	/**
	 * Updates Critter's energy, resets ability to move, and upkeeps the dead.
	 */
	private static void updateRestEnergy(){
		for(int i = population.size()-1; i >= 0; i--){
			Critter checkCritter = population.get(i);
			checkCritter.energy -= Params.rest_energy_cost;
			/* remove rekt Critters (dead) */
			if(checkCritter.energy <= 0){
				population.remove(i);
			}
			/* reset ability to move */
			checkCritter.moved = false;
		}
	}

	/**
	 * Generate Algae Critters object.
	 */
	private static void genAlgae(){
		for(int i = 0; i < Params.refresh_algae_count; i++){
			try{
				makeCritter("Algae");
			}
			catch(InvalidCritterException ice){
				System.out.println("error generating Critter: Algae");
			}
		}
	}

	/**
	 * Carries out the encounter methods. Finds encounters, then carries them out.
	 */
	private static void doEncounters(){
		ArrayList<List<Critter>> encounters = findEncounters();
		battleEncounters(encounters);
	}


	/**
	 * Battles are carried out. See README for more details.
	 * @param encounters ArrayList of Lists Critter objects that have same coordinates
	 */
	private static void battleEncounters(ArrayList<List<Critter>> encounters){
		for(List<Critter> critter : encounters) {

			Critter A = critter.get(0);
			Critter B = critter.get(1);

			if (population.contains(A) && population.contains(B)){

				boolean AChoosesFight = A.fight(B.toString());
				boolean BChoosesFight = B.fight(A.toString());
				boolean critterDied = false;

				/* check if CrittersA/B died from fight */
				if(A.energy <= 0){
					population.remove(A);
					critterDied = true;
				}
				if(B.energy <= 0){
					population.remove(B);
					critterDied = true;
				}

				/* if no one died, and CrittersA/B are on same spot still,
				   then generate two random numbers via dice roll */
				if(critterDied == false && A.x_coord == B.x_coord && A.y_coord == B.y_coord){
					int aAttackPower;
					int bAttackPower;


					if(AChoosesFight == true){
						aAttackPower = getRandomInt(A.energy);
					}
					/* A wants to run away */
					else{
						aAttackPower = 0;
					}

					if(BChoosesFight == true){
						bAttackPower = getRandomInt(B.energy);
					}
					/* B wants to run away */
					else{
						bAttackPower = 0;
					}

					/* "The critter that rolls higher, wins. If tie, arbitrarily select winner */
					Critter winner;
					Critter loser;
					if(aAttackPower >= bAttackPower){
						winner = A;
						loser = B;
					}
					else{
						winner = B;
						loser = A;
					}

					/* if critter loses a fight, then half of loser's energy
					   is awarded to the winner of the fight. The loser is dead.*/
					winner.energy += loser.energy/2;
					population.remove(loser);
				}
			}
		}
	}



	/**
	 * Helper function for worldTimeStep method. Finds all
	 * the critters that share the same spot in the world.
	 * @return Returns ArrayList of Lists of Critters that
	 * 		   share the same spot.
	 */
	private static ArrayList<List<Critter>> findEncounters(){
		ArrayList<List<Critter>> encounters_list = new ArrayList<>();
		/* HashMap to keep track of positions that Critters are in */
		HashMap<String, Critter> critter_positions = new HashMap<>();

		for(Critter critter : population){
			/* create a coordinate key for the HashMap */
			String key = String.format("%d %d", critter.x_coord, critter.y_coord);

			/* if a battle is about to happen */
			if(critter_positions.containsKey(key)){
				ArrayList<Critter> battle_spot = new ArrayList<>();

				/* add both Critters to the battle ground instance */
				battle_spot.add(critter);
				battle_spot.add(critter_positions.get(key));

				/* add to encounter instance to the list of encounters*/
				encounters_list.add(battle_spot);
			}
			/* if no battle found yet */
			else{
				critter_positions.put(key, critter);
			}
		}
		return encounters_list;
	}






	/**
	 * Display board in grid form (LxW).
	 */
	public static void displayWorld() {

		/* initialize board dimensions, accounting for the edge marks */
		String board[][] = new String[Params.world_height + 2][Params.world_width + 2];

		/* display top border */
		board[0][0] = "+";
		for (int column = 1; column < Params.world_width + 1; column++) {
			board[0][column] = "-";
		}
		board[0][Params.world_width + 1] = "+";

		/* display playable spots on the board */
		for (int row = 1; row < (Params.world_height + 1); row++) {
			board[row][0] = "|";
			for (int column = 1; column < (Params.world_width + 1); column++) {
				board[row][column] = " ";
			}
			board[row][Params.world_width + 1] = "|";
		}

		/* display bottom border */
		board[Params.world_height + 1][0] = "+";
		for (int column = 1; column < (Params.world_width + 1); column++) {
			board[Params.world_height + 1][column] = "-";
		}
		board[Params.world_height + 1][Params.world_width + 1] = "+";


		/* prep display all Critters */
		for (Critter critter : population) {
			board[critter.y_coord + 1][critter.x_coord + 1] = critter.toString();
		}


		/* display the Critters, if any */
		for (int row = 0; row < (Params.world_height + 2); row++) {
			for (int column = 0; column < (Params.world_width + 2); column++) {
				System.out.print(board[row][column]);
			}
			System.out.println("");
		}
	}
}