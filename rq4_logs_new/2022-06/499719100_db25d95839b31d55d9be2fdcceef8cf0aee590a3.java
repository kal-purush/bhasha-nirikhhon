import java.util.Scanner;

public class Ejercicio1 {
    public static void main(String[] args) {
        ex1();
    }

    static void ex1(){
        //String cadena1;
        String cadena2;
        String cadena3;
        String cadena4;

        Scanner scanner = new Scanner(System.in); //inicializamos el scanner
        System.out.print("Ingresar cadena 1: ");
        //JAVA 11 usar VAR en vez de String u otro type
        var cadena1 = scanner.nextLine();
        System.out.print("Ingresar cadena 2: ");
        cadena2 = scanner.nextLine();
        System.out.print("Ingresar cadena 3: ");
        cadena3 = scanner.nextLine();
        System.out.print("Ingresar cadena 4: ");
        cadena4 = scanner.nextLine();
        scanner.close(); //cerrando el scanner

        String sEspacio = " ";//AL SER POCO ILEGIBLE USAR + EN VEZ DE CONCAT
        System.out.println(cadena1.concat(sEspacio.concat(cadena2.concat(sEspacio.concat(cadena3.concat(sEspacio.concat(cadena4)))))));
    }
}