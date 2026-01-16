package Java.programacionImperativa.organizacionCodigo.Ejercicios;
//Cálculo del área de un círculo:Escribir un programa en Java que calcule el área de un círculo 
//con un radio de 5 unidades.Buscarlafórmulaconlaquese resuelve.

import java.util.Scanner;
public class Ejercicio22 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        final double pi=3.14;
        double radio =5;
        double area=0;

        System.out.println("");
        area = pi * (radio * radio);
        System.out.printf("El area de un circulo de radio 5 es: %.2f %n", area);
        System.out.println("");

        //Ingresando valores y usando la funcion de cuadrado
        System.out.print("Ingrese en radio del circulo que desea calcular su area ");
        radio = sc.nextDouble();
        area = pi * (Math.pow (radio, 2));
        System.out.printf("El area de un circulo de radio %.2f es: %.2f %n", radio,area);
        System.out.println("");
    }
}