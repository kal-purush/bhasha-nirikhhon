package urjita2;
import java.util.Date;

public class H2 {

	  public static void main(String args[]) {
	     // Instantiate a Date object
	     Date date = new Date();
	     System.out.println("Second Hello World" );
	     // display time and date using toString()
	     //String str = String.format("Current Date/Time : %tc", date );  
	     
	     System.out.printf("%1$s %2$tB %2$td, %2$tY", 
                 "Due date:", date);
	  }
}