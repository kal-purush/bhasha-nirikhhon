/* 
* You have a target number and an array. 
* Check to see it any two numbers in the array add up to the target. 
* Solve it in less than n2 run time
*/

import java.util.HashMap;

public class TargetNumber{
	public static void main(String[] args){
		int[] array = {9, 5, 4, 4, 3, 2, 4};
		int targetNumber = 7;

		System.out.println(isEqualToTargetNum(array, targetNumber));
	}

	public static boolean isEqualToTargetNum(int[] array, int targetNumber){
		HashMap<Integer, Integer> cache = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> cache2 = new HashMap<Integer, Integer>();

		//System.out.println(array.length);
		for(int n=0; n < array.length; n++){
			if((array[n]*2)==targetNumber){
				if(cache.get(array[n])==null){
					cache.put(array[n], 1);
				}
				else{
					System.out.println("true");
					return true;
				}
			}
			if(cache.get(array[n])==null && array[n] < targetNumber){
				cache.put(array[n], 1);
				//System.out.println(cache.get(array[n]));
			}

		}
		return true;
			
	}
}