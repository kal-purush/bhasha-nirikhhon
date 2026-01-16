import java.util.Scanner;
import java.util.InputMismatchException;

public class Main {
    public static void main(String[] args) {
          Scanner scanner = new Scanner(System.in);
        interface OperacionMatematica {
            String ejecutar();
        }
        class Suma extends  {
            public Suma(double operando1, double operando2) {
                super(operando1, operando2);
            }
            public String ejecutar() {
                return "Suma: " + (operando1 + operando2);
            }
        }
        class Resta extends  {
            public Resta(double operando1, double operando2) {
                super(operando1, operando2);
            }
            public String ejecutar() {
                return "Resta: " + (operando1 - operando2);
            }
        }
        class Multiplicacion extends  {
            public Multiplicacion(double operando1, double operando2) {
                super(operando1, operando2);
            }
            public String ejecutar() {
                return "Multiplicación: " + (operando1 * operando2);
            }
        }
        class Division extends  {
            public Division(double operando1, double operando2) {
                super(operando1, operando2);
            }
            public String ejecutar() {
                if (operando2 == 0) {
                    return "Error: división por cero";
                } else {
                    return "División: " + (operando1 / operando2);
                }
            }
        }

        while (true) {
            System.out.println("Calculadora de operaciones matemáticas");
            System.out.println("1. Suma");
            System.out.println("2. Resta");
            System.out.println("3. Multiplicación");
            System.out.println("4. División");
            System.out.println("0. Salir");
            System.out.print("Seleccione la operación (0-4): ");

            int opcion;
            try {
                opcion = scanner.nextInt();
                scanner.nextLine(); // Consume el salto de línea
            } catch (InputMismatchException e) {
                System.out.println("Entrada no válida. Por favor, ingrese un número válido.");
                scanner.nextLine(); // Consume la entrada incorrecta
                continue;
            }

            if (opcion == 0) {
                break;
            } else if (opcion < 0 || opcion > 4) {
                System.out.println("Opción no válida.");
                continue;
            }

            System.out.print("Ingrese el primer operando: ");
            double operando1 = scanner.nextDouble();

            System.out.print("Ingrese el segundo operando: ");
            double operando2 = scanner.nextDouble();

            OperacionMatematica operacion = null;
            switch (opcion) {
                case 1:
                    operacion = new Suma(operando1, operando2);
                    break;
                case 2:
                    operacion = new Resta(operando1, operando2);
                    break;
                case 3:
                    operacion = new Multiplicacion(operando1, operando2);
                    break;
                case 4:
                    operacion = new Division(operando1, operando2);
                    break;
                default:
                    break;
            }

            if (operacion != null) {
                System.out.println(operacion.ejecutar());
            }
        }
    }
}