package week2.day1.Assignment_Arrays;

public class FindDuplicateElements {

	public static void main(String[] args) {      

		int [] arr = new int [] {1, 2, 8, 3, 4, 2, 7, 8, 1};   		 

		System.out.println("Duplicate elements in given array: ");  
		for(int i = 0; i < arr.length; i++) { 			
			for(int j = i + 1; j < arr.length; j++) {  				 
				if(arr[i] == arr[j])  {					
					System.out.println(arr[j]);  
				}				
			}  			
		}  		
	}  
}