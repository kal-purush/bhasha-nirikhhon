package Wordarray;

import java.util.Scanner;

/**
 * Created by user on 07.12.2017.
 */
public class Wordarray {
    public void start(){
        Scanner scanner = new Scanner(System.in);

        System.out.println("Введите количество слов");
        int iWordCount = scanner.nextInt();

        String aWordArray[] = new String[iWordCount];
        for (int i = 0; i < iWordCount-1; i++) {
            System.out.println("Ввыдите " + i+1 + " слово массива:");
            aWordArray[i] = scanner.next();
        }
    }

}