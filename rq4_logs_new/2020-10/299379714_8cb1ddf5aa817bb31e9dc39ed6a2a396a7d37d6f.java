import java.util.Scanner;

public class MethodsExercises {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter an integer: ");
        int firstInt = sc.nextInt();
        System.out.println("Enter another integer:");
        int secondInt = sc.nextInt();
        int result = 0;
        addTwoNumbers(firstInt,secondInt);
        subtractTwoNumbers(firstInt, secondInt);
        multiplyTwoNumbers(firstInt, secondInt);
        divideTwoNumbers(firstInt, secondInt);
        modulusTwoNumbers(firstInt, secondInt);
        System.out.println("Multiplying your two integers results in a product of " + multiplyStarless(firstInt, secondInt, result));
    }
    public static void addTwoNumbers(int a, int b) {
        int sum = a + b;
        System.out.printf("The sum of %d and %d = %d\n", a, b, sum);
    }

    public static void subtractTwoNumbers(int a, int b) {
        int difference = a - b;
        System.out.printf("%d take away %d = %d\n", a, b, difference);
    }

    public static void multiplyTwoNumbers(int a, int b) {
        int product = a * b;
        System.out.printf("The product of %d times %d = %d\n", a,b,product);
    }

    public static void divideTwoNumbers(int a, int b) {
        int quotient = a / b;
        System.out.printf("The quotient of %d divided by %d = %d\n", a,b,quotient);
    }

    public static void modulusTwoNumbers(int a, int b) {
        int remainder = a%b;
        System.out.printf("The remainder of %d divided by %d is %d.\n", a,b,remainder);
    };

    public static int multiplyStarless(int a, int b, int sum){

        if(b > 0){
            sum += a;
            return multiplyStarless(a, b-1, sum);
        }else{
            return sum;
        }
    }

    public static int getInteger(int min, int max){
        
    }


}