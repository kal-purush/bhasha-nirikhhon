/*
 * Name: Summing
 * @author: Shoji Moto
 * @version: 1.0
 * Date: 10/2/15
 * 
 * Compiler: BlueJ
 * Platform: Windows 8
 * 
 * Description: Program uses a do while loop to manipulate numbers
 * 
 * Learned how to use a do while loop
 */


import java.io.*;
import java.util.*;

public class Summing {
    public static void main(String[] args) {
        Scanner values = new Scanner(System.in);
        System.out.print("Enter Starting Value: ");
        int start = values.nextInt(); //input assigned as start, will change later
        System.out.println();
        
        System.out.print("Enter Ending Value: ");
        final int END = values.nextInt(); //input assigned as END
        System.out.println();
        
        final int SAVEDSTART = start; //To be printed in output
        
        int sum = 0;
        int count = 0;
        
        do { //BEGIN values loop
            System.out.println(start); //print list of numbers
  
            sum = sum + start; 
            count = count + 1;
            
            start++;
        } while (start <= END); //END values loop, continue looping until start > END
        
        double avg = Math.round(((double)sum / count) * 100.0) / 100.0; //Finding the average
        
        System.out.println("Sum of the numbers " + SAVEDSTART + ".." + END + " is " + sum);
        System.out.println("The average of the numbers " + SAVEDSTART + ".." + END + " is " + avg);
    }
}

/*
 * Output:
 * 
Enter Starting Value: 5

Enter Ending Value: 8

5
6
7
8
Sum of the numbers 5..8 is 26
The average of the numbers 5..8 is 6.5

Enter Starting Value: 4

----

Enter Ending Value: 12

4
5
6
7
8
9
10
11
12
Sum of the numbers 4..12 is 72
The average of the numbers 4..12 is 8.0

----

Enter Starting Value: 1

Enter Ending Value: 1

1
Sum of the numbers 1..1 is 1
The average of the numbers 1..1 is 1.0

----

Enter Starting Value: 2

Enter Ending Value: 3

2
3
Sum of the numbers 2..3 is 5
The average of the numbers 2..3 is 2.5

----

Enter Starting Value: 4

Enter Ending Value: 6

4
5
6
Sum of the numbers 4..6 is 15
The average of the numbers 4..6 is 5.0
*/