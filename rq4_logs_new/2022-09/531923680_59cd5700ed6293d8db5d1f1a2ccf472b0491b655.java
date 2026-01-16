import java.util.Scanner;

public class OperadorTernario {
    public static void main(String[] args) {
        int max =  0;
        Scanner scanner = new Scanner(System.in);

        System.out.print("Ingrese un número: ");
        int num1 = scanner.nextInt();

        System.out.print("Ingrese un segundo número: ");
        int num2 = scanner.nextInt();

        System.out.print("Ingrese un tercer número: ");
        int num3 = scanner.nextInt();

        max = (num1>num2)? num1: num2 ;
        max = (num3>max)? num3: max;
        System.out.println("El número mayor es: " + max);


    }
}