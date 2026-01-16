package evaluacionfinal;

public class Main {
    public static void main (String[] args) {
        // Ejercicio 1
        int nro1 = 2 ;
        int nro2 = 6 ;

        if (nro1 < nro2) {
            System.out.println("El numero " + nro2 + " es mayor que el numero " + nro1);
        } else if (nro1 > nro2){
            System.out.println("El numero " + nro1 + " es mayor que el numero " + nro2);
        } else {
            System.out.println("Ambos numeros son iguales");
        }
    }
}