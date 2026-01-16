package com.numericsystool.flycat.numericsys;
import java.util.Scanner;
import java.text.DecimalFormat;
/*
 [a,b] son intervalos
 * f: funcion
 * a: limite inferior
 * b: limite suoerior
 * it: contador de iteraciones
 * it_limite: numero maximo de itraciones
 */

public class Biseccion {
    /*definicion de una funcion de variable*/
    public static double f(double x){
        return Math.sqrt(x)-Math.cos(x);
    }

    public static void funcion(String[] args) {
        Scanner a1 = new Scanner(System.in);
        Scanner b1 = new Scanner(System.in);

        System.out.println("Intervalo inferior:");
        double a=a1.nextDouble();
        System.out.println("Intervalo superior");
        double b=b1.nextDouble();
        //darle formato de 5 decimales
        DecimalFormat forma = new DecimalFormat("##.#####");
        double  c;
        int it=0;
        /*[a,b] es el intervalo F(a)*f(b)<0 */
        if(f(a)*f(b)>0)
        {
            System.out.println("No cumple. trate otro intevalo. ");
        }
        else{

            while(1>0){
                //iteración
                it=it+1;

                c=(a+b)/2;
                System.out.println(+it+"  "+ forma.format(a)+ "  "+ forma.format(c)+"  "+forma.format(b) );
                System.out.println("  "+forma.format(f(a))+"  "+forma.format(f(c)));

                if(it==14){
                    System.out.println("La raiz aproximada es r = "+ forma.format(c));
                    System.out.println("El valor de f en r = " + forma.format(f(c)));
                    return;
                }
                if(f(a)*f(c)<0)
                {
                    b=c;
                }
                else{
                    a=c;
                }
            }
        }
    }
}