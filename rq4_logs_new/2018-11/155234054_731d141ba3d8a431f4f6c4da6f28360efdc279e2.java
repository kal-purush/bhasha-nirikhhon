package com.qa.testbase;
public class stepCountClass {
	// A simple recursive program to find n'th fibonacci number 
    static int fib(int n) 
    	{ 
	        if (n <= 1){
	            return n; 
	            } 
	               
	        else {
	        	return fib(n-1) + fib(n-2); 
	        }

    	} 

	//Returns number of ways to reach s'th stair 
		protected static int countWays(int s) 
		{ 
		    return fib(s + 1); 
		} 
		

}