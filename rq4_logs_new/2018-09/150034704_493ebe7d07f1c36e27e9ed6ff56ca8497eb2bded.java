/**
 * The <code>InvalidInputException</code> class extends the Exception class and is used to indicate
 * that the user has entered input that does not fit within the described conditions.
 *
 *
 * @author Piotr Milewski
 *    email: piotr.milewski@stonybrook.edu
 *    Stony Brook ID: 112291666
 **/

public class InvalidInputException extends Exception{

    public InvalidInputException(){
	//default message
	super("User input does not fit within the described conditions.");
    }

    public InvalidInputException(String message){
	//passed message
	super(message);
    }    
}