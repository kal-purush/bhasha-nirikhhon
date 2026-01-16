/**
 * NegativeAgeException.java
 * version 1.1
 * Compiled on 16th Aug 2017
 */
//package declaration

package session62;
/**
 * This class will illustrate the user-defined exception class NegativeAgeException which is declared as a subclass of Parent class Exception.
 * This class will generate a user-defined exception called NegativeAgeException if the user inputs negative value for age.
 * 
 * @AUTHOR CHHAYA YADAV
 */
//Child class declaration

public class NegativeAgeException extends Exception{

//instance variable declaration
	
	int userAge ;

//parameterized constructor declaration
	
	public NegativeAgeException( int age ){
		
		userAge = age ;

//if user inputs invalid age which is negative 		
		
		if(userAge < 0)
		{
			System.out.println("Invalid User Input for age ! Please provide positive Number for age ! ");
		}
//if user inputs age as  zero , which is also invalid
		
		else if(userAge == 0)
		{
			System.out.println("User age cannot be zero ! Please provide positive Number for age ! ");
		}
//if user inputs positive age		
		else
			System.out.println("Valid User Positive Age ! " );
		
	}



}