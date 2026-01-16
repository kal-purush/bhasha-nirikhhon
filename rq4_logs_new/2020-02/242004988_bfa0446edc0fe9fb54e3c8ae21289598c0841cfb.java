/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.revisaojava;

/**
 *crie um método que faça  a multiplicação de um array de números
 * @author igor.valgoi
 */
public class exercicio3 {
   
    public static void main (String[]arqs){
      
      int[] array = {2,5};
      int resultado = multiplicacao(array);
      
      
      System.out.println("Multiplicação: " + resultado);
    }
     
    
    private static int multiplicacao(int[] numeros){
    int resultado = 1;
    
    
     for (int i : numeros){
        resultado *= i;
    }
       return resultado;
    }
  
}
    