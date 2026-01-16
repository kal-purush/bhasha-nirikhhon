/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.leapfrog.v;

import java.util.Scanner;

/**
 *
 * @author Abhash
 */
public class Isvowel {
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        while(true){
        System.out.println("Enter an alphabet");
    
        Scanner s1=new Scanner(System.in);
        char ch=s1.next().charAt(0);
        if(ch=='a'||ch=='e'||ch=='i'||ch=='o'||ch=='u'){
        
            System.out.println("IS Vowels" );
        }
        else {
            System.out.println("Consonant");
        }
    
        System.out.println("Do you want to continue[Y/N]");
            String chh = s1.next();
            if (chh.equalsIgnoreCase("n")) {
                System.exit(0);
        }
        
    }
}
}

   