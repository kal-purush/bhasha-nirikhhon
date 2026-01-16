import java.util.*;

public class calculations {

    public static int sum(int x, int y){
        int z = x + y;
        return z;
    }
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter the first number:");
        int x = sc.nextInt();
        System.out.print("Enter the second number:");
        int y = sc.nextInt();
        System.out.println("Sum of two numbers is : "+sum(x,y));
        sc.close();
    }
    
}