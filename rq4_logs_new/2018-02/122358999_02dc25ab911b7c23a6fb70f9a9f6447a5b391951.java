import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Exercise6
{

    /*
        Write a method countUnique that accepts a list of integers as a parameter
        and returns the number of unique integer values in the list.
        Use a set as auxiliary storage to help you solve this problem.
        For example, if a list contains the values [3, 7, 3, –1, 2, 3, 7, 2, 15, 15],
        your method should return 5.The empty list contains 0unique values.
    */

    public static void main(String[] args)
    {
        ArrayList<Integer> list = new ArrayList<Integer>();

        list.add(3);
        list.add(7);
        list.add(3);
        list.add(-1);
        list.add(2);
        list.add(3);
        list.add(7);
        list.add(2);
        list.add(15);
        list.add(15);

        System.out.println("Original list: " + list);
        countUnique(list);

    }

    public static void countUnique(ArrayList<Integer> list)
    {
        Set<Integer> set = new HashSet<Integer>(list);  //  makes a hashset with the first list, eliminating duplicates

        System.out.println("Unique numbers in list: " +  set.size());   // prints the size of the set
    }

}