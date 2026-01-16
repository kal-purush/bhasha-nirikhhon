import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * This program uses insertion sort to
 * sort an array.
 *
 * @author  Sarah Andrew
 * @version 1.0
 *
 * @since 2023-02-05.
 */

public final class InsertSort {

    /**
     * This is a private constructor used to satisfy the
     * style checker.
     *
     * @exception IllegalStateException Utility class.
     * @see IllegalStateException
     */
    private InsertSort() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * This is the main method.
     *
     * @param args Unused.
     */
    public static void main(String[] args) {
        // Pass path to file as parameter.
        final File file = new File("input.txt");
        final File fileOut = new File("output.txt");

        // Create a list of strings.
        final List<String> listOfStrings = new ArrayList<String>();

        // Declare variable
        String stringList;

        // Usage of try catch to detect error.
        try {
            // Create FileWriter object to write to file.
            final FileWriter fW = new FileWriter(fileOut);
            // Create Scanner object to read from file.
            final Scanner sc = new Scanner(file);
            // Create PrintWriter object to write to file.
            final PrintWriter write = new PrintWriter(fW);

            while (sc.hasNextLine()) {
                // Read line as string.
                stringList = sc.nextLine();
                // Add to list.
                listOfStrings.add(stringList);

                // Checking for empty lines
                // blank inputs.
                if (stringList.isEmpty()) {
                    // Display to user.
                    write.println("No integers found on line.");
                    System.out.println("No integers found on line.");
                    continue;
                }
            }
            // Convert from list to array.
            final String[] arrayOfStr = listOfStrings.toArray(new String[0]);

            // Convert all elements in array to integers.
            // To do so, loop through each element & convert
            // each string.
            for (int counter = 0; counter < arrayOfStr.length; counter++) {
                try {
                    final String[] nums = arrayOfStr[counter].split(" ");
                    final int[] arrayNum = new int[nums.length];
                    for (int counter2 = 0; counter2 < nums.length; counter2++) {
                        arrayNum[counter2] = Integer.parseInt(nums[counter2]);

                    }

                    // Call function.
                    final int[] insertionSort =
                        insertSort(arrayNum);

                    // Display & write to file.
                    System.out.println(Arrays.toString(insertionSort));
                    write.println(Arrays.toString(insertionSort));

                } catch (NumberFormatException error) {
                    // Display error to user.
                    System.out.println("Valid input only!");
                    write.println("Valid input only!");
                    continue;
                }
            }

            // Closes scanner & writer.
            write.close();
            sc.close();

        } catch (IOException error) {
            // Displays error to user.
            System.out.println("An error occurred: "
                    + error.getMessage());
        }
    }

    /**
    * This function uses bubble sort to
    * sort an array.
    * Referenced from video.
    *
    * @param listNum passed.
    * @return listNum.
    */
    public static int[] insertSort(int[] listNum) {
        // Usage of for loop to access each element in array.
        for (int counter1 = 1; counter1 < listNum.length - 1; counter1++) {
            // Declare variable.
            final int current = listNum[counter1];
            int temp = counter1 - 1;

            // Usage of loop that checks if current is bigger than
            // previous, switching positions if so, to sort.
            while (temp >= 0 && listNum[temp] > current) {
                listNum[temp + 1] = listNum[temp];
                // Decrement temp.
                temp = temp - 1;
            }
            listNum[temp + 1] = current;
        }
        // Return to main.
        return listNum;

    }
}