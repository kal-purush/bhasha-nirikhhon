package PROG.P_5.P_5_3;

import java.util.Arrays;

public class P_5_3_1_matrizUnosYCeros {
    // Daniel Alonso Lázaro - 2022
    /*
    Hacer un programa Java, que:
        a) Crea una matriz de 10x10 (filas * columnas) y nombre 'tabla'.
        b) Carga la matriz de manera que las filas pares se rellenan con 1 y las filas impares con 0.
        c) Una vez inicializada la matriz muestra su contenido en pantalla.
    */
    public static void main(String[] args) {
        int[][] matriz = new int[10][10];
        for(int i = 0; i < matriz.length; i++) {
            for(int j = 0; j < matriz[i].length; j++) {
                if(i%2 == 0){
                    matriz[i][j] = 0;
                }else{
                    matriz[i][j] = 1;
                }
            }
        }
        System.out.println("Matriz de unos y ceros: ");
        for(int[] ints:matriz){
            System.out.println(Arrays.toString(ints));
        }
    }
}