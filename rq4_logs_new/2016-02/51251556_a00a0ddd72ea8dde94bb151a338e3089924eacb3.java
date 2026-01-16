package cse360assign1;

/**
 * Class to hold integers in an array. Non-duplicate values 
 * are inserted in order. If the array is full the largest value
 * is dropped off the end. Bounds checking is done.
 * 
 * @author Adam Charney PIN 213 for CSE360 Spring 2016
 * 
 */

public class OrderedIntList {
	
	/** Array to hold sorted integers */
	private int[] array;
	
	/** Place holder to aid with current size and check bounds */
	private int counter;
	
	/**
	 * Creates an array of integers, count is ten by default
	 */
	
	OrderedIntList () {
		
		array = new int[10];
	}
	
	/**
	 * insert - Inserts parameter into array in ascending numerical 
	 * order. If list is full and parameter is less than the largest
	 * value, largest value is dropped off array, otherwise number 
	 * is not inserted.
	 * 
	 * @param insertNum Number to be inserted*/
	
	public void insert (int insertNum) {
		
		int index = 0;
		int follow = 0;
		
		while(index < counter && insertNum > array[index])
		{
			index++;
			follow++;
		}
		
		if(insertNum != array[index]) 
		{
			for (index = counter; index > follow; index--)
				array[index] = array[index - 1];
			
			//check for space in array or that insertion is less than greatest
			if(counter < array.length - 1 || insertNum < array[counter])
				array[follow]= insertNum;
		
			if(counter < array.length - 1)
				counter++;
		}
	}
	
	/**
	 * print - Prints all integers stored in the array, maximum of
	 * five integers per line.
	 */
	
	public void print () {
		
		if (counter == 10)
		{
			for (int index = 0; index <= counter; index++)
			{
				//move to new line after 5 entries
				if (index % 5 == 0)
					System.out.println();
				
				System.out.print(array[index] + "\t"); 
			}
		}
		else
		{
			for (int index = 0; index < counter; index++)
			{
				//move to new line after 5 entries
				if (index % 5 == 0)
					System.out.println();
				
				System.out.print(array[index] + "\t"); 
			}
		}
		}
	}
}
