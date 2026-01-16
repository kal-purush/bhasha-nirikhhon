package week1.day1.assignments;

public class ArmstrongNumber {

	
	public static void main(String[] args) {
	    int calculated=0,remainder,original;  
	    int nput=23; 
	    original=nput;  
	    while(nput>0)  
	    {  
	    remainder=nput%10;  
	    nput=nput/10;  
	    calculated=calculated+(remainder*remainder*remainder);  
	    }  
	    if(original==calculated)  
	    System.out.println(calculated +" " + "is armstrong number");   
	    else  
	        System.out.println(calculated +" " + "is Not armstrong number");   
	   }  
	
	}
		
	
	/*	int input = 153;
		int calculated=0;
		int remainder;
		int original;
		original=input;
		while(input>0)
		{
				
			remainder=(input%10);
			//System.out.println(remainder);
			input=(input/10);
			//System.out.println(quotient);
			input=original;
			calculated=calculated+(remainder*remainder*remainder);
			System.out.println(input);
			
		}
		
		if(original==input)  
		    System.out.println("armstrong number");   
		    else  
		        System.out.println("Not armstrong number");   
		   
		
	}
*/