//*******************************************************************
// writeNums - Recursion - Ch12, Ex2, p831
//  
// Write a method that takes an integer n as a parameter and prints to the
//   console the first n integers starting with 1 in sequential order,
//   separated by commas.
// Include an IllegalArgumentException if passed a value less than 1.
// 
// By: Michael Gilson
// Date: 5/9/2020
//*******************************************************************

public class writeNums {

	public static void main(String[] args) {
		
// call the writeNbrs method 3 times to test for various outputs
		writeNbrs(5);			// should return "1, 2, 3, 4, 5"
		System.out.println();	
		writeNbrs(12);			// should return "1, 2, 3, 4, 5 ,6 ,7 ,8 ,9 ,10, 11, 12"
		System.out.println();
		writeNbrs(0);			// should return IllegalArgumentException error

	}
	
// create method that accepts an integer as a parameter
	public static void writeNbrs (int n) {
		
// Establish Illegal Argument Exception with message output
		if (n < 1) {
			throw new IllegalArgumentException("can't be less than 1.");
			
// Establish base case - if n ever equals 1, it will print "1".
		} else if (n == 1) {
			System.out.print("1");
			
// Set up recursive loop: while n is larger than 1, it will go back through
//	the method with n-1, until "1" is reached. It will then print "1", and 
//  return to the previous loop where it left off, printing ", 2", then return
//  to the next previous loop until "n" is reached, thus printing the numbers
//  in sequential order.
		} else {
			writeNbrs(n-1);
			System.out.print(", " + n);
			
		} // End else
	} // End method
} // End class