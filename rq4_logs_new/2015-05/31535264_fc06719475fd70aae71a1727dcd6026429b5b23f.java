/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amstrongsayilar;

/**
 *
 * @author taha
 */
public class AmstrongSayilar {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
         System.out.println("100 ile 1000 arasındaki Amstrong sayılar : ");
        int a,b,c,d;
        for (int i = 100; i <= 999; i++) {
             a = i / 100;
             b = (i - a * 100) / 10;
             c = (i - a * 100 - b * 10);

             d = (a * a * a) + (b * b * b) + (c * c * c);

            if (i == d) {
                System.out.println(i);
            }
        }
    }
    
}