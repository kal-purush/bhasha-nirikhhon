package Java.programacionImperativa.organizacionCodigo.Ejercicios;
//Cálculo de la energía cinética: Escribir un programa en Java que calcule la energía cinética de un objeto
//en movimiento, dados su masa y su velocidad. Utilizar la fórmula: E = (1/2) * m * v^2, donde E es la 
//energía cinética, m es la masa del objeto y v es la velocidad.

import java.util.Scanner;
public class Ejercicio26 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        double masa=0,velocidad =0, energia=0;
        System.out.println("");
        System.out.print("Ingrese el valor de la masa: ");
        masa = sc.nextDouble();
        System.out.print("Ingrese el valor de la velocidad: ");
        velocidad = sc.nextDouble();
        System.out.print("La Energia cinetica de un objeto cuya masa es " + masa + " y velocidad es " + velocidad + " es: ");
        energia =  0.5 * masa * (velocidad*velocidad);//E = (1/2) * m * v^2
        System.out.printf("%.2f ", energia);
    }
}