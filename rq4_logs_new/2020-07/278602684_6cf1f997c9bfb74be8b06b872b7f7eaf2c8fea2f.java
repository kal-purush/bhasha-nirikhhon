import java.util.Scanner;

public class string_number {
    public static void main(String[] args) {
        // Initialized Scanner
        Scanner scan1 = new Scanner(System.in);

        // Reading Input
        System.out.print("Enter the the no. of integers in string: ");
        int N = scan1.nextInt();
        System.out.print("Enter the string with numbers: ");
        scan1.nextLine(); // To discard /n character
        String str = scan1.nextLine();

        // Initializing Variables
        String temp = "";
        int count = 0;
        int[] numbers = new int[N];

        // Looping through the string
        for(int i=0;i<str.length();i++) {
            // If ',' is not present concatenate to temp variable
            if (str.charAt(i) != ',') {
                temp = temp + str.charAt(i);
            }
            // If ',' found then convert temp to int and store in array and increase array index count and clear temp string
            else {
                numbers[count] = Integer.parseInt(temp);
                count++;
                temp = "";
            }
        }
        // Store the last integer of the string to array
        numbers[count] = Integer.parseInt(temp);

        // Looping thorough the array and printing the values
        for(int j=0;j<numbers.length;j++) {
            System.out.println(numbers[j]);
        }
    }
}


/* OUTPUT
Enter the the no. of integers in string: 4
Enter the string with numbers: 14,97,125,2
14
97
125
2
*/