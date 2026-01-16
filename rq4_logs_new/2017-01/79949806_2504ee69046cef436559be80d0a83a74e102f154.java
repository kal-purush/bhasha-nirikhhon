
import java.util.NoSuchElementException;

public class MyPriorityQueue <E extends Comparable<E>> {
	
	private E[] itemsArray;
	private int numItems;
	private final int INIT_LENGTH = 100;
	private int length;
	
	public MyPriorityQueue () {
		
		itemsArray = (E[]) (new Comparable[INIT_LENGTH]);
		numItems = 0;
		
	}
	
	public MyPriorityQueue (int length) {
	
		itemsArray = (E[]) (new Comparable[length]);
		numItems = 0;
		
	}
	
	void insert(E item) {
		
		 numItems++;
   	 //if array if full, double the size of the array
   	 if(numItems == itemsArray.length) {
   		 itemsArray = expand(itemsArray);
   	 }
   	 //insert the new item as a leaf and then swap the item to the proper 
   	 //place
   	 itemsArray[numItems] = item;
   	 ComparisonSort.testmoves++;
   	 int k = numItems;
   	 while(k > 1) {
   		 if (item.compareTo(itemsArray[k / 2]) > 0) {
   			itemsArray[k] = itemsArray[k/2];
   			itemsArray[k/2] = item;
   			ComparisonSort.testmoves++;
   			ComparisonSort.testmoves++;
   		 }
   		 k = k / 2;
   	 }
	}
	
	/**
    * A helper method to expand the array
    * @param oriArray: original array
    * @return the expanded array
    */
   private E[] expand(E[] oriArray) {
  	 
  	 int largerSize = 2 * oriArray.length; //new array size
  	 itemsArray = (E[])(new Comparable[largerSize]); //new array
  	 for (int i = 1; i < oriArray.length; i++) { //copy items to new array
  		 itemsArray[i] = oriArray[i];
  		ComparisonSort.testmoves++;
  	 }
  	 return itemsArray;
  	 
   }
	
	boolean isEmpty() {
		return numItems == 0;
	}
	
	E removeMax() {
		if(numItems == 0) {
  		 throw new NoSuchElementException();
  	 }
  	 
  	 E max = itemsArray[1]; //root (first item of array) is the max
  	//move the last item to the first place, then move it to the right place
  	 itemsArray[1] = itemsArray[numItems]; 
  	 int k = 1;
  	 //compare the first item with its children, swap with the larger child
  	 //if it is smaller than either of its child; continue until it is bigger
  	 //than both of its children
  	 while(2 * k < numItems) {
  		 if(itemsArray[k].compareTo(itemsArray[2*k]) < 0
  				 || itemsArray[k].compareTo(itemsArray[2*k+1]) < 0) {
  			 E largerChild;
  			 if(itemsArray[2*k].compareTo(itemsArray[2*k + 1]) < 0) {
  				 largerChild = itemsArray[2*k + 1];
  				 itemsArray[2*k + 1] = itemsArray[k];
  				 itemsArray[k] = largerChild;
  				ComparisonSort.testmoves++;
  				ComparisonSort.testmoves++;
  				ComparisonSort.testmoves++;
  				 k = 2 * k + 1;
  			 } else {
  				 largerChild = itemsArray[2*k];
  				 itemsArray[2*k] = itemsArray[k];
  				 itemsArray[k] = largerChild;
  				ComparisonSort.testmoves++;
  				ComparisonSort.testmoves++;
  				ComparisonSort.testmoves++;
  				 k = 2 * k;
  			 }
  		 } else {
  			 break;
  		 }
  		 
  	 }
  	 	numItems--;
      return max;
	}
	
	int size() {
	   return numItems;	
	}	

}

