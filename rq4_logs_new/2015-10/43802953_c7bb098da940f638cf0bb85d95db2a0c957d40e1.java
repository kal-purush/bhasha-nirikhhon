package org.meltwater.java.dataStructures;

public class Test {

	public static void main(String[] args) {
		/**
		 * BetterArray test
		 */
		BetterArray<Object> array = new BetterArray<Object>();
	    array.add(2);
	    array.add(3);
	    array.shift(5);
	    array.insert(0, 1);
	    array.index(3);
	    array.get(1); // 2
	    array.isEmpty();
	    
	    int[] newElements = {4, 5, 6};
	    array.add(newElements);
	    
	    array.contains(2); // true
	    array.contains(10); // false

	    array.index(2); // 1
	    array.index(10); // -1 
	    
//	    array.get(10000); // ???
	    
	    System.out.println(array.contains(2));
		System.out.println("Size: " + array.size());
		System.out.println("Is Empty: " + array.isEmpty());
		System.out.println(array.dispString());
		
		/**
	     * Unit tests the SET data type.
	     */
	        Set<String> set = new Set<String>();

	        // insert some keys
	        set.add("MEST");
	        set.add("MEST");    // overwrite old value
	        set.add("2016A");
	        set.add("2016B");
	        set.add("Generosity");
	        set.add("Positivity");
	        set.add("Standards");
	        
	        System.out.println(set.contains("MEST"));
	        System.out.println(set.contains("2016B"));
	        System.out.println();
	        System.out.println();

//	        for (String string : set) {
//	        	System.out.println(set);
//	}	
	        /**
	         * unit tests for BST
	         * @param args
	         */
	           Integer[] a = {1,5,2,7,4};
	           BST<Integer> bst = new BST<Integer>();
	           for(Integer n : a) bst.add(n);

	           for(Integer n : bst) System.out.print(n);
	           System.out.println();

	           System.out.println(bst);     
    }
}