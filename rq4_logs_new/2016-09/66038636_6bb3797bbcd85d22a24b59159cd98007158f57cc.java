/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 *
 * @author pichon
 */
public class Registration {
    
    public static void main(String[] args){
        Set<String> db = new HashSet<>();
        Scanner scan = new Scanner(System.in);
        int cant = scan.nextInt();
        String entrada, aux;
        entrada = scan.nextLine();        
        for (int i = 0; i < cant; i++) {
            entrada = scan.nextLine();
            if (db.contains(entrada)) {
                for (int j = 1; j < 100; j++) {
                    aux = entrada+(Integer.toString(j));
                    if (!db.contains(aux)) {
                        db.add(entrada+(Integer.toString(j)));
                        System.out.println(entrada+(Integer.toString(j)));
                        break;
                    }
                }
            }else {
                db.add(entrada);
                System.out.println("OK");
            }
        }
    }
}