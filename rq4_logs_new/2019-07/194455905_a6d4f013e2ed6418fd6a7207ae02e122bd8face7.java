/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package arreglos2;

import java.util.Scanner;
import javax.swing.JOptionPane;

/**
 *
 * @author BREN
 */
public class Arreglos2 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Scanner entrada = new Scanner(System.in);
        
        int numero,i;
        
        numero = Integer.parseInt(JOptionPane.showInputDialog("Digite de que tamaño de su arreglo: ")); //se guarda el valor que da el usurio en la varianle "numero"
        
        
        int[] edades = new int[numero]; //Se declara el arreglo con el numero de elementos que decidió el usuario
        
        
        //El siguiente for lo que hace es guardar cada elemento que de el usuario.
        for(i=0;i<numero;i++)
        {
            System.out.print("Digite la edad: ");
            edades[i] = entrada.nextInt();//se guarda en el arreglo.
        }
        
        //El siguiente for solo nos va a mostrar las edades que ingresó el usuario.
        System.out.println("::Las edades que ingresaste fueron las siguientes::");
        for(i=0;i<numero;i++)
        {
            System.out.print(edades[i]+" ");
            
        }
        
        
        
    }
    
}