import java.util.Scanner;

public class SimpleCalculator{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter: 1 for Addition\nEnter: 2 for Subtraction\nEnter: 3 for Multiplication\nEnter: 4 for Division\nEnter: 5 for Modulus");
        int n = sc.nextInt();
        System.out.print("Enter a: ");
        int a = sc.nextInt();
        System.out.print("Enter b: ");
        int b = sc.nextInt();
        System.out.println();
        switch (n) {
            case 1:
                System.out.print("Result: ");
                System.out.println(a+b);
                break;
            case 2:
            System.out.print("Result: ");
                System.out.println(a-b);
                break;
            case 3:
            System.out.print("Result: ");
                System.out.println(a*b);
                break;
            case 4:
            System.out.print("Result: ");
                System.out.println(a/b);
                break;
            case 5:
            System.out.print("Result: ");
                System.out.println(a%b);
                break;
        
            default:
            System.out.println("Please enter a valid number");
                break;
        }
    }
}