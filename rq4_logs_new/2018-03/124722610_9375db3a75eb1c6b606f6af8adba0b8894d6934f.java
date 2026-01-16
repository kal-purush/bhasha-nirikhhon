package randomPermutation;

import java.util.ArrayList;
import java.util.Random;

public class RandomPermutation
{	
	/** Makes a random permutation of all the elements in the list,
	 * where no element is repeated.
	 * @param list a list that you want to permutate. Will be changed.
	 */
	public static void randomlyPermutate(int[] list) 
	{
		Random rand = new Random();
		for(int rem = list.length; rem > 0; rem--) 
			swap(list,rem-1,rand.nextInt(rem));			
	}
	
	/** Makes a random permutation in the array src outputted to a given array dest,
	 * where no element is repeated.
	 * @param src the source array. Will not be changed as a result of this call.
	 * @param dest the destination array. Will be changed as a result of this call.
	 */
	public static void randomlyPermutate(int[] src, int[] dest) 
	{
		copyArray(src,dest);
		randomlyPermutate(dest);
	}
	
	/** Creates an array of randomly selected elements from src, where no element
	 * is repeated.
	 * @param src the source array
	 * @param quantity the quantity of elements that will be randomly selected
	 * @return an array of randomly selected elements from src
	 */
	public static int[] randomlySelect(int[] src, int quantity) 
	{
		int[] temp = new int[src.length];
		randomlyPermutate(src,temp);
		
		int[] result = new int[quantity];
		copyArray(temp,result);
		return result;
	}
	
	
	/** Will generate a random set of tuples, where the two dimensional array
	 * contains lists, each of which define the possible values of a component of
	 * a tuple. No component will be repeated.
	 * This method does not create unique orderings of tuples(they will be ordered such
	 * that the 0th components in the first list align with the 0th component in the
	 * resulting tuples).
	 * @param lists the list of possible components of tuples. Assumed to have lists of all equal size
	 * @return a random set of tuples, where no component is repeated
	 */
	public static ArrayList<Integer[]> selectRandomTuples(int[][] lists)
	{
		// Each row in the lists will be a list of possible values for a component
		// of a tuple. While each column can be considered a tuple. Now we just need
		// to shuffle around most of them(except for the 0th one, as the method will
		// not generate unique orderings)
		ArrayList<Integer[]> result = new ArrayList<Integer[]>(lists.length);
		int[][] copy = new int[lists.length-1][lists[0].length];
		for(int i = 1; i < lists.length; i++) 
		{
			copyArray(copy[i],lists[i]);
			randomlyPermutate(lists[i]);
		}
		for(int i = 0; i < lists.length; i++) 
		{
			Integer[] tuple = new Integer[lists[0].length];
			tuple[0] = lists[0][i];
			for(int j = 1; j < tuple.length; j++) 
				tuple[j] = lists[j][i];
			result.add(tuple);
		}
		return result;
	}
	
	/** Creates a random grouping of elements based on a given format.
	 * @param src The elements being grouped together
	 * @param format Describes how many elements are in each group. The value 
	 * at index i of format describes the number of elements for group i.
	 * @return A two dimensional array where each array i contains the elements
	 * in group i. 
	 */
	public static int[][] assignGroups(int[] src, int[] format)
	{
		int[][] groups = new int[format.length][];
		int[] temp = new int[src.length];
		copyArray(src,temp);
		randomlyPermutate(temp);
		int ind = 0;
		for(int i = 0; i < groups.length; i++) 
		{
			groups[i] = new int[format[i]];
			for(int j = 0; j < format[i]; j++)
				groups[i][j] = temp[ind++];
		}
		return groups;
	}
	
	private static void swap(int[] list, int a, int b) 
	{
		int temp = list[a];
		list[a] = list[b];
		list[b] = temp;
	}
	
	private static void copyArray(int[] src, int[] dest) 
	{
		int end = Math.min(src.length, dest.length);
		for(int i = 0; i < end; i++)
			dest[i] = src[i];
	}
	
	private static void printList(int[] list) 
	{
		for(int i = 0; i < list.length; i++)
			System.out.print(list[i] + " ");
		System.out.println();
	}
	
	// Testing to make sure that the likelihood of any value appearing anywhere
	// Is normal, returns a two dimensional array detailing how frequently an index i
	// had the value j occur as freq[i][j]
	private static int[][] testPermutations(int size, int numTrials) 
	{
		int[][] freq = new int[size][size];
		int[] original = new int[size];
		for(int i = 0; i < size; i++)
			original[i] = i;
		
		int[] test = new int[size];
		for(int i = 0; i < numTrials; i++) 
		{
			randomlyPermutate(original,test);
			for(int j = 0; j < size; j++)
				freq[j][test[j]]++;
		}
		return freq;
	}
	
	private static void print2DArray(int[][] list) 
	{
		for(int i = 0; i < list.length; i++) 
		{
			for(int j = 0; j < list[i].length; j++) 
				System.out.print(list[i][j] + " ");
			System.out.println();
		}
	}
	
	public static void main(String[] args) 
	{
//		int[] list = {0,1,2,3,4,5,6,7,8,9,10};
//		randomlyPermutate(list);
//		printList(list);
		int[][] freq = testPermutations(10,10000);
		print2DArray(freq);
	}
}