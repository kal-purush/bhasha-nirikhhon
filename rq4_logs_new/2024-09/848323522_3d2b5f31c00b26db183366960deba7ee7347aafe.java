import java.util.Random;
import java.util.Scanner;

public class Ejercicio5 {
    public static void main(String[] args) {
        Random random = new Random();
        Scanner scanner = new Scanner(System.in);

        int numeroAleatorio = random.nextInt(101);
        int intento;
        int intentos = 0;

        do {
            System.out.print("Adivina el número (entre 0 y 100): ");
            intento = scanner.nextInt();
            intentos++;

            if (intento < numeroAleatorio) {
                System.out.println("El número es mayor.");
            } else if (intento > numeroAleatorio) {
                System.out.println("El número es menor.");
            }

        } while (intento != numeroAleatorio);

        System.out.println("Has adivinado el número.");
        System.out.println("Número de intentos: " + intentos);

        scanner.close();
    }
}