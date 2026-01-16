package Programming;

import java.util.Scanner;

public class MergeSort {
	
	public static void main (String[] args) {
		
		// variable for the user input
		int sizeOfArray;
		
		Scanner userInput = new Scanner(System.in);
		
		// get the user input
		sizeOfArray = userInput.nextInt();
		
		int MyArray [] = new int [sizeOfArray], i;
		
		for (i=0; i<sizeOfArray; ++i) {
			
			MyArray[i] = (int) (Math.random()*100);
			System.out.print(MyArray[i] + " ");
						
		}
		
		MyArray = MergeSort(MyArray);
		
		System.out.println();
		
		for(i=0; i<sizeOfArray; ++i) {
			
			System.out.print(MyArray[i] + " ");
			
		}
		
		
	}
	
	public static int[] MergeSort(int [] MyArray){
		
		int sizeOfArray = MyArray.length;
		
		if (sizeOfArray == 1) {
			
			return MyArray;
		}
		else {
			
			// Threshold
			int halfOfArray = (int) Math.floor(MyArray.length/2);
			
			int [] FirstHalfOfArray = new int [halfOfArray];
			int [] SecondHalfOfArray = new int [sizeOfArray - halfOfArray];
			
			for(int i=0; i<sizeOfArray; ++i){
				
				if (i<halfOfArray) {
					
					FirstHalfOfArray[i] = MyArray[i];
				}
				else {
					
					SecondHalfOfArray[i - halfOfArray] = MyArray[i];
					
				}
			}
				
				FirstHalfOfArray = MergeSort(FirstHalfOfArray);
				SecondHalfOfArray = MergeSort(SecondHalfOfArray);
				
				MyArray = Merge(FirstHalfOfArray, SecondHalfOfArray);
				
				return MyArray;
				
		}
	}
	
	public static int [] Merge(int [] FirstHalfOfArray, int [] SecondHalfOfArray) {
		
		int sizeOfFirstHalfOfArray = FirstHalfOfArray.length;
		int sizeOfSecondHalfOfArray = SecondHalfOfArray.length;
		
		int Combine [] = new int [sizeOfFirstHalfOfArray + sizeOfSecondHalfOfArray];
		
		int ItemFirstHalfOfArray = 0;
		int ItemSecondHalfOfArray = 0;
		
		for (int i=0; i< sizeOfFirstHalfOfArray + sizeOfSecondHalfOfArray; ++i) {
			
			if (ItemFirstHalfOfArray == sizeOfFirstHalfOfArray) {
				
				Combine[i] = SecondHalfOfArray[ItemSecondHalfOfArray];
				++ItemSecondHalfOfArray;
				
			}
			else if (ItemSecondHalfOfArray == sizeOfSecondHalfOfArray) {
					
					Combine[i] = FirstHalfOfArray[ItemFirstHalfOfArray];
					++ItemFirstHalfOfArray;
							
			}
			else if (FirstHalfOfArray[ItemFirstHalfOfArray] > SecondHalfOfArray[ItemSecondHalfOfArray]) {
				
				Combine[i] = SecondHalfOfArray[ItemSecondHalfOfArray];
				++ItemSecondHalfOfArray;
				
			}
			else  {
				
				Combine[i] = FirstHalfOfArray[ItemFirstHalfOfArray];
				++ItemFirstHalfOfArray;
				
			}			
		}
		
		return Combine;
	}

}