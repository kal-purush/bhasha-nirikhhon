package hands_on_3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Jonah {
//	this class instantiates an object of the multiply class and invokes the respective overloaded functions 

	public static void main(String args[]){
//		local variables and the MUltiply class object declaration
		String choice = null,int1 = null,int2=null,flt1=null,flt2=null;
		int resultInt;
		float resultFloat;
		Multiply multiplyObject = new Multiply();
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		// Ask for user choice
		System.out.println("Enter choice \n 1. Multiplying integers \n 2. Multiplying floats");
		try {
			choice = reader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Switch case for the choice entered by the user
		//	incase of integer the integer multiply function is invoked and
		//	incase of float the float multiply function is invoked
		switch(Integer.parseInt(choice)){
			case 1: System.out.println("Enter the Integer numbers to multiply");
					try {
						int1 = reader.readLine();
						int2 = reader.readLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					resultInt= multiplyObject.multiply(Integer.parseInt(int1), Integer.parseInt(int2));
					System.out.println("The result of Integer multiplication is :"+resultInt);
					break;
			case 2: System.out.println("Enter the Float numbers to multiply");
					try {
						flt1 = reader.readLine();
						flt2 = reader.readLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					resultFloat= multiplyObject.multiply(Float.parseFloat(flt1), Float.parseFloat(flt2));
					System.out.println("The result of Float multiplication is :"+resultFloat);
					break;
			default : 
				System.out.println("Incorrect choice entered");
					
			
		}
			
	}
}