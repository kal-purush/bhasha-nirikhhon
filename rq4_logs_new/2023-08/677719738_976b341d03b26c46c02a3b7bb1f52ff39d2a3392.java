import java.util.Scanner;

import static java.lang.System.in;
import static java.lang.System.out;

public class LabActivity1
{
    static final Scanner scanner = new Scanner(in);
    static String firstName;
    static String lastName;
    static String middleName;

    static String name;
    static int[] nameIndexValues;
    static int[] a1;
    static int[] a2;
    static int[][] outerArray;
    static int num1;
    static int num2;

    static int number;
    public static void main(String[] args)
    {
        start();
    }

    public static void isNameValid()
    {
        boolean validName = false;

        while (!validName)
        {
            try
            {
                out.println("First name: ");
                firstName = scanner.nextLine(); //Saippuakivikauppias // Use that palindrome to validate the code
                out.println("Middle name: ");
                middleName = scanner.nextLine();
                out.println("Last name: ");
                lastName = scanner.nextLine(); //Saippuakivikauppias // Use that palindrome to validate the code

                if (firstName.length() <= 30 && firstName.matches("[a-zA-Z\\s]+")
                        && lastName.matches("[a-zA-Z\\s]+") && middleName.matches("[a-zA-Z]+"))
                {
                    validName = true;
                }
                else
                {
                    throw new IllegalArgumentException();
                }
            }
            catch (IllegalArgumentException e)
            {
                out.println("Invalid input. \n First name should only be less than 30 characters " +
                        "\n Middle name should not contain any whitespace " +
                        "\n Names especially last name should not contain any special characters " +
                        "(e.g., numbers and /*-#)");
            }
        }
    }

    public static void isNumberValid()
    {
        boolean validNum = false;
        while (!validNum)
        {
            out.println("\nEnter an integer number from 0 to 1000 only");
            try
            {
                number = Integer.parseInt(scanner.nextLine());
                if (number >= 0 && number <= 1000)
                {
                    validNum = true;
                    out.println("\nValid random number: " + number + "\n");
                }
                else
                {
                    throw new IllegalArgumentException();
                }
            }
            catch (IllegalArgumentException e)
            {
                out.println("\n Invalid input. Please enter integers from 0 to 1000 only. \n");
            }
        }
    }


    public static void validateTwoNumbers()
    {
        boolean validNum = false;

        while (!validNum)
        {
            try
            {
                out.println("Enter two numbers ");
                out.println("Number 1: ");
                num1 = scanner.nextInt();
                out.println("Number 2: ");
                num2 = scanner.nextInt();

                if (num1 < outerArray.length && num2 < outerArray.length)
                {
                    validNum = true;
                }
                else
                {
                    throw new IllegalArgumentException();
                }
            }
            catch (IllegalArgumentException e)
            {
                out.println("\n Invalid input. Numbers is out of bound of the outer array \n");
            }
        }
    }
    public static void start()
    {
        isNameValid();
        isNumberValid();
        int n = 0;
        while (firstName.length() != lastName.length())
        {
            n++;
            out.println("\nAttempt " + n + "\n");
            isNameValid();

            if (n == 5)
            {
                out.println("Exiting the program...");
                break;
            }
        }

        if (firstName.length() == lastName.length())
        {
            evaluateLastName();
        }
    }

    public static void evaluateLastName()
    {
        name = lastName.toLowerCase().replaceAll(" ", "");
        convertLowerCaseValuesInIndex();
        a1 = nameIndexValues;
        // out.println(Arrays.toString(a1)); // Use this for debugging

        boolean palindromeFound = isPalindrome();

        if (palindromeFound == true)
        {
            name = middleName.toLowerCase().replaceAll(" ", "");
            convertLowerCaseValuesInIndex();
            a2 = nameIndexValues;
            // out.println(Arrays.toString(a2)); // Use this for debugging

            outerArray = printOuterArray(new int[][] {a1, a2});

            validateTwoNumbers();
            out.println("Element: " + outerArray[num1][num2]);
        }
    }

    public static int[][] printOuterArray(int[][] outerArray)
    {
        int n = 0;
        out.println("outer array = [");
        //Outer loop
        for (int row = 0; row < outerArray.length; row++)
        {
            n++;
            out.print("\t a" + n + " = [");
            //Inner loop
            for (int col = 0; col < outerArray[row].length; col++)
            {
                if (col == outerArray[row].length-1)
                {
                    out.print(outerArray[row][col] + "]");
                }
                else
                {
                    out.print(outerArray[row][col] + ", ");
                }
            }
            out.println();
        }
        out.println("]");
        return outerArray;
    }
    public static void convertLowerCaseValuesInIndex()
    {
        nameIndexValues = new int[name.length()];
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            int charIndex = c - 'a' + 1;
            // out.println(c + " = " + charIndex); // Use this for debugging
            nameIndexValues[i] = charIndex;
        }
    }

    public static boolean isPalindrome()
    {
        int found = 0;
        boolean isPalindrome = false;
        // out.println("Palindromes to be check: " + Arrays.toString(nameIndexValues)); // Use this for debugging
        int n = nameIndexValues.length - 1;
        for (int i = 0; i < name.length()/2; i++)
        {
            if (nameIndexValues[i] == nameIndexValues[n])
            {
                found++;
            }
            n--;
        }
        if (found == name.length()/2)
        {
            isPalindrome = true;
        }
        return isPalindrome;
    }
}