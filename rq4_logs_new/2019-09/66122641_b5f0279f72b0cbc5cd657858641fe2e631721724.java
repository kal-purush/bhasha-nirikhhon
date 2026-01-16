package com.practice.declarations.variable;

public class ClassVariableDeclaration {

	//This is accessible only in this class, No where else
	private int privateScopeNumber;
	
	//This is accessible only in the class and within package
	float defaultScopeNumber;

	//This is accessible only in the class and within package and any inherited classes	
	protected double protectedScopeNumber;
	
	//This is accessible only in the class and within package and any inherited classes	
	public String publicScopeLiteral;
	
	//This static is accessible only in this class, No where else
	private static int staticPrivateScopeNumber;
	
	//This static is accessible only in the class and within package
	static float staticDefaultScopeNumber;

	//This static is accessible only in the class and within package and any inherited classes	
	protected static double staticProtectedScopeNumber;
	
	//This static is accessible only in the class and within package and any inherited classes	
	public static String staticPublicLiteral;
	
	public static void main(String [] args) {
		
		ClassVariableDeclaration object = new ClassVariableDeclaration();
		ClassVariableDeclaration diffObject = new ClassVariableDeclaration();
		//These below are instance variables, To access or set the value of the variable
		//First you must create a object using new operator and the constructor like above
		
		object.privateScopeNumber = 2; //Proof that the private variable is accessible
		
		//Also note any whole number is a integer by default in Java
		
		//Also note any decimal number is a double by default in Java
		//If we have assign to a float variable then we need to type cast first.
		//Type casting is making the value compatible to the variable
		//Here 4.5 is double so type casting to float so that it is compatible to float
		object.defaultScopeNumber = (float)4.5;  //Proof that default is accessible within class.
		
		object.protectedScopeNumber = 6.09;  //Proof that protected is accessible within class.
		
		object.publicScopeLiteral = "Welcome";  //Proof that public is accessible within class.
		
		//For static we can directly access or set as given without creating Object.
		
		ClassVariableDeclaration.staticPrivateScopeNumber = 5;
		ClassVariableDeclaration.staticDefaultScopeNumber = (float)8.77;
		ClassVariableDeclaration.staticProtectedScopeNumber = 7.077;
		ClassVariableDeclaration.staticPublicLiteral ="This is a Static String";
		

		System.out.println("Trying to access the static variables set without Object"+ClassVariableDeclaration.staticPrivateScopeNumber);
	
		//It is not a good practice to access a static variable using the object, just showed for demonstration.
		System.out.println("Actually we did not set against the object, static is shared across objects and here is the proof = "+object.staticPrivateScopeNumber);
		System.out.println("Actually we did not set against the diffObject, static is shared across objects and here is the proof = "+diffObject.staticPrivateScopeNumber);
		
		
	}

}