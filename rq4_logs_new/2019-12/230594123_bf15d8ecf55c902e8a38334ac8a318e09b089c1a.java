package demos;
/**
 * 
 * This is my first java application
 *
 */
public class Hello {
	
	//this is single line comment
	
	/*
	 * multi line comment
	 */
	
	/**
	 * 
	 * This is main method for documentation
	 */
	
	//public -- so that Jvm can access
	//void -- because jvm(interpreter is not going to do anything with a 
	//returned value+ it's inbuild in interpreter
	//to search for main with void as return type
	//static Hello.main() without creating object can access main method
	public static void main(String[] args) {
		byte b=(byte)130;
		System.out.println(b);
		
		short s=(short)32768;
		System.out.println(s);
		
		char c='\u0620';
		System.out.println(c);
		
		System.out.println("Hello World");
	}
}
	