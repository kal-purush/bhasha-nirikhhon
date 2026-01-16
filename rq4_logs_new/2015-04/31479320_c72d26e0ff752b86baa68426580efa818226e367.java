package Interview;

public class GS {

	/**
	 *numbers 0-9999, find one missing number
	 *find number!=index, can use binary search
	 */
	
	/**
	 * class A update some object, want to notify class B
	 * use Observer design pattern
	 */
	
	/**
	 * describe how to use one array to implement three stacks
	 */
	
	/**
	 * check if a number is power of 10, like 100, 1000, 10
	 * should think about negative integers
	 */
	
	/**
	 * maximum path sum, 
	 * follow up: how to do it in a bottom-up way
	 * a graph like a tree, which a node has two parents and no empty nodes in each level, 
	 * how to solve one middle node been visited multiple times problem - I said use a map to track visited nodes
	 */
	
	/**
	 * unsorted array [5,6,3,2,9,7,10]
	 * how to find the largest number - one global max variable, O(n)
	 * how to find the second largest number - two global max variable, O(n)
	 * how to find the Kth largest number in O(n) time?
	 * use a modified quick sort, select a pivot randomly, swap smaller numbers to left of pivot, larger numbers to right of pivot.
	 * if pivot index is K, just return the pivot.
	 * if pivot index smaller than k, remove the left side, quick sort the right part.
	 * if pivot index larger than k, remove the right side, quick sort the left part.
	 * it'll take n+n/2+n/4+n/8... = 2n time
	 */
	
	/**
	 * sql find largest id
	 * sql find second largest id
	 * database normalization
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}