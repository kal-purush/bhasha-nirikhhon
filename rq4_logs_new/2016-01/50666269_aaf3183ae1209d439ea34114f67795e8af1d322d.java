/* Collin Mullis
 * Coded January 29th 2016
 * Code for Lab # 2
 */
package edu.saintjoe.cmullis;

// Define class named Lab 2
public class Lab2 {
	// Define Constants and variables
	private static final int NUM_REGIONS = 6;
	private static int[] weightArray;
	private static String[] regionArray;
	
	// give  a size to each array.
	weights = new int[NUM_REGIONS];
	regions = new String[NUM_REGIONS];
	
	for (int i = 0; i < NUM_REGIONS; i++) {
		weightArray[i] = 0;
		regionArray[i] = new String();
	}

	
	
	// Method that accepts an Integer for Index, a String for Region name, and an Integer for Average Weight
	public Lab2(int newInd, String newReg, int newAvg) {
		
		regionArray[newInd] = newReg;
		weightArray[newInd] = newAvg;
		
	}
	public String toString() {
		String output = new String(); 
		output = "Region\t Weight\n";
		for(int n=0; n < NUM_REGIONS; n++) {
			output = output + "/t" + regionArray[n] + "\t" + weightArray[n] + "\n";
			return output;
		}
	}
	
	public static void main(String[] args) {
		
		new Lab2(0, "Africa", 134);
		new Lab2(1, "Asia", 127);
		new Lab2(2, "Europe", 156);
		new Lab2(3, "Latin America", 150);
		new Lab2(4, "Northern America", 178);
		new Lab2(5, "Oceania", 163);
		
		
		regionArray.toString();
		weightArray.toString();
		
		
		
	}
}