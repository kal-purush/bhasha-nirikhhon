import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
//Exercise 1:
//Ask the user to enter for names in an array. Print the array
//
//        ArrayList<String> nameList = new ArrayList<String>();
//
//        Scanner scan = new Scanner(System.in);
//
//        nameList.add("John");
//        nameList.add("Kenn");
//        nameList.add("Kevin");
//        nameList.add("Jason");
//
//        for (String name : nameList) {
//            System.out.println( name );
//        }
//    }
//    }


//Exercise 2:
//Create the array [5,3,7, 4]. Pass the array to a new function to do the following:
//
//        ArrayList<Integer> numbers = new ArrayList<Integer>();
//
//        numbers.add(5);
//        numbers.add(3);
//        numbers.add(7);
//        numbers.add(4);
//Print the array.
//        for (Integer number : numbers) {
//            System.out.println( number );
//        }
//Print the 3rd index of the numberList.
//        System.out.println(numbers.get(3));

//Delete the second index.
//        numbers.remove(2);
//        System.out.println(numbers);


//Print the 3rd element again.
//        System.out.println(Integer.valueOf(3));
//    }
//    }



//Exercise 3:
//Create the array ["Bob", "John", "Kenn", "Kevin"].
//
        ArrayList<String> teachers = new ArrayList<String>();

        teachers.add("Bob");
        teachers.add("John");
        teachers.add("Kenn");
        teachers.add("Kevin");

//Print the array.
        for (String teacher : teachers) {
            System.out.println( teacher );
        }
        //Remove “Kenn” from the array.
        teachers.remove(String.valueOf("Kenn"));
        System.out.println(teachers);

        //Print the size of the array.
        System.out.println(teachers.size());

//Check to see if the array contains “Kevin”. If so, get the index of “Kevin” to print.
        if (teachers.contains("Kevin")) {
            System.out.println(teachers.get(3));
        }
    }

    }


//Exercise 4:
//Ask the user to enter 5 numbers. Put them in an array and print the sum.
//

//Exercise 5:
//Create an array in the main function. Create two functions, min and max, to find the minimum and maximum values. Do not use a min/max function.
//
//Exercise 6:
//Create a program that will add all numbers given to a program until the number 0 is pressed. Then return the total of all numbers.
//
//Exercise 7:
//Allow a user enter as many strings as they want to until they enter the character 'q'. Check to see if the strings are similar to a palindrome.







