/*
 * filename: EulerTotientFunction.java
 * 
 * Version: 1.0 (10/15/2015)
 * 
 * Revisions: $Log Initial Version$
 */

import java.util.Scanner;


/**
 * @author DhruvGala
 *
 * The following code computes the Euler's Totient function. The primary algorithmic
 * flow is initially determine if the number is prime, if so return that number - 1
 * if not, then determine what all prime numbers starting from 2 divide this number,
 * record a count of each divisions with a particular prime number and multiple the 
 * product to compute the Euler's Totient Function.
 * 
 */
public class EulerTotientFunction {

	/**
	 * This code determines if the number given as parameter is prime.
	 * 
	 * @param n
	 * @return
	 */
	public static boolean isPrime(int n) {
	    for(int i=2;i<n;i++) {
	        if(n%i==0){
	            return false;
	        }
	    }
	    return true;
	}
	
	
	/**
	 * The logic to divide the last remainder of the number by increasing prime
	 * numbers until a resultant remainder is found.
	 * 
	 * @param prime
	 * @param remainder
	 * @return
	 */
	public static int[] counter(int prime,int remainder){
		if(remainder%prime != 0){
			return new int[]{1,remainder};
		}
		else{
			int count = 0;
			while(remainder%prime == 0){
				remainder = remainder/prime;
				count++;
			}
			int[] returnThis = {(int)(Math.pow(prime, count)
					- Math.pow(prime, count - 1)),remainder};
			
			return returnThis;
		}
	}
	
	
	/**
	 * The actual logic of computing the Euler's Totient funtion is governed
	 * by this method
	 * 
	 * @param n
	 * @return
	 */
	public static int findTotients(int n){
		if(isPrime(n)){
			return n-1;
		}
		else{
			int remainder = n,product = 1,prime = 2;;
			while(!isPrime(remainder)){
				if(isPrime(prime)){
					int[] InternalProduct = counter(prime,remainder);
					product *= InternalProduct[0];
					remainder = InternalProduct[1];
				}
				prime++;
			}
			product *= (remainder-1);
			
			return product;
		}
		
	}
	
	
	
	/**
	 * Takes input from the user.
	 * 
	 * @return
	 */
	public static int feedInput(){
		Scanner takeInput = new Scanner(System.in);
		
		System.out.print("Enter the number n: ");
		int n = takeInput.nextInt();
		takeInput.close();
		
		return n;
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		int n = feedInput();
		System.out.println("Euler's Totient function of "+n+": "+findTotients(n));
	}
}