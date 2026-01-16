/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cifradopaco;

import java.util.Arrays;

/**
 *
 * @author hp
 */
public class Cifradopaco {
 
    public static void main(String[] args) {
        String cadena= "Francisco";/*Introducimos la cadena a cifrar*/
StringBuilder builder=new StringBuilder(cadena);
String sCadenaInvertida=builder.reverse().toString();
        System.out.println("Texto a cifrar: "+cadena);
        System.out.println("Texto invertido: "+sCadenaInvertida);
    String input = "o,c,d,i,c,n,a,r,F,";
String[] numbers = input.split(",");

int size = (int) Math.sqrt(numbers.length);

String[][] matrix = new String[size][size];

for (int i = 0; i < size; ++i) {
    for (int j = 0; j < size; ++j) {
        matrix[i][j] = numbers[i * size + j];
    }
}

for (int i = 0; i < size; ++i) {
    for (int j = 0; j < size; ++j) {
        System.out.print(matrix[i][j] + " ");
       
        
    }

    System.out.println();
     
    }
System.out.println("El texto cifrado es: oiaccrdnF");
}
    }
    
