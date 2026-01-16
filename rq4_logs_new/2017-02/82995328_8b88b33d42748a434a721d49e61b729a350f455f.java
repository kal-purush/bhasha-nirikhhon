/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reglafalsa;

import java.util.Scanner;

/**
 * Programa que determine una de las raíces reales de 
 * la función: f(x) = (1 –0.6x)/x
 *
 * @author ENRIKE
 */
public class ReglaFalsa {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);

        //valor inicial y final del intervalo de busqueda
        double vInicial, vFinal;

        //Error aproximado maximo 
        double error;

        System.out.println("--------METODO DE LA REGLA FALSA-------");
        System.out.println();
        System.out.println("Nota: El programa no acepta `puntos` solo `comas` ");
        System.out.println("--------------------------------------------------------");
        System.out.println();
        System.out.print("Ingrese el valor inicial del intervalo: ");
        vInicial = teclado.nextDouble();
        System.out.println();
        System.out.print("Ingrese el valor final del intervalo: ");
        vFinal = teclado.nextDouble();
        System.out.println();
        System.out.print("Ingrese el error aproximado maximo: ");
        error = teclado.nextDouble();

        reglaFalsa(vInicial, vFinal, error);
    }

    /**
     * recibe como parámetros valores inicial y final del intervalo de búsqueda
     * y el error aproximado máximo. La función calcula y despliega los valores
     * de la raíz, el valor de la función para esa raíz, el error aproximado
     * para la última iteración y el número de iteraciones requerida para
     * encontrar la raíz.
     *
     * @param vInicial
     * @param vFinal
     * @param error
     */
    public static void reglaFalsa(double vInicial, double vFinal, double error) {

        double xi = vInicial, xf = vFinal;
        double er = 100, xr, xrl;
        boolean h = true;
        int i = 0;

        //calculo de xr
        xr = xi - ((xf - xi) / (f(xf) - f(xi))) * f(xi);
        xrl = xr;
        do {

            //se calcula el valor de xr
            xr = xi - ((xf - xi) / (f(xf) - f(xi))) * f(xi);

            //calculo del error despues de la 2da iteracion
            if (i != 0) {
                er = Math.abs(((xr - xrl) / xr) * 100);
            }

            //si es menor de 0, asigna el valor de xr a xf
            if (f(xi) * f(xr) < 0) {
                xf = xr;
            }
            //si es mayor de 0, asigna el valor de xr a xi
            if (f(xi) * f(xr) > 0) {
                xi = xr;
            }

            //si el error ya se alcanzo se imprimen los resultados
            if (er <= error) {
                h = false;

                System.out.println("--------RESULTADOS------------");

                System.out.println("Valor de la raiz: " + Math.rint(xr * 1000000) / 1000000);
                System.out.println("Valor de la funcion: " + Math.rint(f(xr) * 1000000) / 1000000);
                System.out.println("Iteraciones: " + i);
                System.out.println("Error:" + Math.rint(er * 1000000) / 1000000 + "%");
                break;
            }

            //se guarda xr para el proximo calculo del error
            xrl = xr;

            //conteo de las iteraciones
            i++;

        } while (h);
    }

    /**
     * recibe como parámetro el valor de una abscisa (x) y regresa la ordenada
     * (y) de acuerdo a la función: f(x) = (1 – 0.6x)/x. Esta función será
     * llamada por la función reglaFalsa cada vez que desee obtener el valor de
     * la función.
     *
     * @param x
     * @return
     */
    public static double f(double x) {
        double y;

        y = (1 - (0.6 * x)) / x;

        return y;
    }

}