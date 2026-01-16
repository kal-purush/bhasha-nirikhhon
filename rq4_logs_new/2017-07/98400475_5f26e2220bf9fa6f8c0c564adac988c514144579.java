/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package caeasercipher;

import java.util.Scanner;

/**
 *
 * @author oyas
 */
public class CaesarCipher {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        Scanner sc = new Scanner(System.in);
        System.out.println("Lütfen bir anahtar giriniz: ");
        int key = sc.nextInt();
        sc.nextLine();
        
        System.out.println("Lütfen mesajı giriniz: ");
        String msg = sc.nextLine();
        
        System.out.println("Şifreleme için E, çözümleme için D giriniz: ");
        String op = sc.next();
        
        if(op.equals("E")){
            System.out.println(encrypt(msg, key));
        }
        else if(op.equals("D")){
            System.out.println(decrypt(msg, key));
        }
        else{
            System.out.println("Hatalı bir operasyon seçtiniz.");
        }
        
    }
    
    public static String encrypt(String msg, int key){
        if(key < 0) key = 26-(-key%26);
        String result = "";
        for(int i = 0; i < msg.length();i++){
            char ch = msg.charAt(i);
            if(Character.isLetter(ch)){
                if(Character.isUpperCase(ch)){
                    result += (char)('A' + (ch + key - 'A')%26);
                }
                else{
                    result += (char)('a' + (ch + key - 'a')%26);
                }
            }
            else{
                result += ch;
            }
        }
        return result;
        
    }
    
    public static String decrypt(String msg, int key){
           return encrypt(msg, -key);
    }
}