package Chapter2;

import java.util.*;

/**
 * Program to convert celcius to farenheight
 *
 * @author Kaleb Alsobrook
 */
public class C2_1 
{
    /**
     * Main Method
     *
     * @param args arguments from command line prompt
     */
    public static void main(String[] args) 
    {
        Scanner imput = new Scanner(System.in);
        
        System.out.print("Enter a degree in celcius");
        double celcius = imput.nextDouble();
        
        //Convert celcius to ferenheight
        double fahrenheit = (9.0 / 5) * celcius + 32;
        System.out.println("Celcius " + celcius + " is " + fahrenheit + " in fahrenheit");
                
    }
}