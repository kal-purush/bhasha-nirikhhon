/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package task4;

import java.util.Scanner;

/**
 *
 * @author win
 */
public class TASK4 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        // TODO code application logic hereScanner input = new Scanner(System.in);
       int c;
       System.out.println("Enter 1 for Hi, 2 for Hey, 3 for Hello");
       System.out.println("Enter Choice");
          c = input.nextInt()
       if(c==1)
           System.out.println("You chose Hi");
       else if(c==2)
           System.out.println("You chose Hey");
       else if(c==3)
       System.out.println("You chose Hello");
       else
           System.out.println("Invalid choice");
    }
    
}