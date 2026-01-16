package solutions.exceptions;

import java.util.Scanner;

public class Exceptions {


      public static String sum(String a, String b) throws RuntimeException  {

          if (a == "" || b == "") {

              throw new IllegalArgumentException("One of the strings is empty");
          }

        if(a.matches("\\d*")&& b.matches("\\d*")){
            return a+b;

        }


        else
        {
            throw new IllegalArgumentException("The input data does not contain only digits.");
        }
    }

      public static void readNumbers() throws OutofIntervalException
      {
          int value;
          Scanner in = new Scanner(System.in);

          System.out.println("Enter a number between 100 and 200");
          value = in.nextInt();
          in.close();
          if (value < 100 || value > 200)
          {
              throw new OutofIntervalException("The number is not between 100 and 200");
          }
      }

}