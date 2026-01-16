import java.util.Scanner;
public class Average {
    public static void main(String[] args) {
        System.out.println("Enter a series of numbers. Enter a negative number to quit.");
        Scanner scan = new Scanner(System.in);
        double input; double sum = 0;
        int i = 0;
        while(true) {
            input = scan.nextDouble();
            if(input < 0) {break;}
            sum = sum + input;
            i++;
        }
        double avg = sum / i;
        System.out.println("You entered " + i + " numbers averaging " + avg + ".");
    }
}