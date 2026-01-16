//Data Structures
//Name: Carolyn Savas
//Lab 3A


import static java.lang.System.*;
import java.util.Scanner;
import java.util.NoSuchElementException;
import java.util.InputMismatchException;
import java.lang.ArithmeticException;
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.lang.IllegalStateException;

public class TryCatchRunner
{
  public static void main (String[] arg) throws Exception 
  {
    Scanner fileR = new Scanner(new File("Lab_3A.txt"));
    Scanner kb = new Scanner(System.in);
    ArrayList<Integer> nums = new ArrayList<Integer>();
    int sum = 0;
      for(int i = 0; i < 10; i++)
      {
        try{
          nums.add(fileR.nextInt());
        }
        catch(NoSuchElementException e){
          out.println("There is not enough data for the whole arraylist");
          fileR.close();
        }
        catch(IllegalStateException e){
          out.println("Closed at "+ i +".");
        }
      }
    
      
      for(int i = 0; i <nums.size();i++)
      {
        sum += nums.get(i);
      }
      out.print("Enter non negative integer: ");
      int divisor = 0;
      try{
        divisor = kb.nextInt();
      }
      catch(InputMismatchException e){
        out.println("Thats not an integer, please fix it");
        divisor = (int)(Double.parseDouble(kb.next()));
      }
      try{
        out.println("The sum of the data divided by " + divisor + " is " + sum/divisor);
      }
      catch(ArithmeticException e){
        if(divisor==0)
          out.println("its impossible to divide by 0");
        out.println("The original sum was "+sum);
      }
    
  }
}