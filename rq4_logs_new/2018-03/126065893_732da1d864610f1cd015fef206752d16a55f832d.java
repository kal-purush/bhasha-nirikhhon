package main;

public class RandomGenerator {
	
	/**
	 * Returns a random positive whole number between the minimum and maximum values specified.
	 * @param range
	 * @return
	 */
	public static int randomInteger(int range) {
		return (int)(Math.random() * range);
	}
	
	
	/**
	 * Returns a random positive whole number between the minimum and maximum values specified.
	 * @param min The lowest possible value
	 * @param max The highest possible value
	 * @return A random number between min and max
	 *  
	 */
	public static int randomInteger(int min, int max) {
		return (int)(Math.random() * (max - min) + min);
	}
	
	
	public boolean randomBoolean() {
		boolean temp = false;
		if(Math.random() > 0.5) {
			temp = true;
		}return temp;
	}
	
}