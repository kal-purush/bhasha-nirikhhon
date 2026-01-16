/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package calculadora;

import java.util.Scanner;

/**
 *
 * @author labing506
 */
public class DeDecimal {
    int numero;
    public void Decimal_octal(){
       
        System.out.println("Decimal a Octal");
        Scanner sc=new Scanner(System.in);
        System.out.println("\nIngrese un Numero: ");
        numero=Integer.parseInt(sc.nextLine());
        String octal = Integer.toOctalString(numero);
        System.out.println("La Representacion Octal es: " + octal);
    }
          
    public void Decimal_binario(){
            
    }
    public void Decimal_hexal(){
        System.out.println("Decimal a Hexadecimal");
       Scanner sc=new Scanner(System.in);
        System.out.println("\nIngrese un Numero: ");
        numero=Integer.parseInt(sc.nextLine());
        String hexa = Integer.toHexString(numero);
        System.out.println ("La Representacion Hexadecimal es: " + hexa);
    }
      
   
}
        
        
    
    

    