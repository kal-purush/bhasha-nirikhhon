/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lineerrecursivesearch;

import java.util.Random;
import java.util.Scanner;

/**
 *
 * @author taha
 */
public class LineerRecursiveSearch {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        int uzunluk = 0;
        System.out.println("Dizinin uzunluğunu giriniz :");
        Scanner girilenSayi = new Scanner(System.in);      
        uzunluk = girilenSayi.nextInt();    
        int dizi[] = new int[uzunluk];    
        for (int i = 0; i < dizi.length; i++) {    
            System.out.println((i + 1) + ". elamanı giriniz");
            dizi[i] = girilenSayi.nextInt();
        }
        int arananSayi = 0;
        System.out.println("aramak istediğiniz sayıyı giriniz :");
        arananSayi = girilenSayi.nextInt();
        lineerSearch(dizi, 0, dizi.length, arananSayi);

    }

    public static void lineerSearch(int[] arr, int fIndex, int lIndex, int searchNum) {

        if (fIndex == lIndex) {

            System.out.println("Aranan sayı bulunumadı !");

        } else {

            if (arr[fIndex] == searchNum) {
                System.out.println("aranan sayı bulundu");
                System.out.println("aranan sayı =" + searchNum);
                System.out.println("sayının bulunduğu sıra = " + (fIndex+1));

            } else {

                lineerSearch(arr, fIndex + 1, lIndex, searchNum);

            }

        }

    }

}