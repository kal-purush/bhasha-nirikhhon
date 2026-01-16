package com.ua.tsaran.homework20;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        TranslateMap map = new TranslateMap();
        Scanner scanner = new Scanner(System.in);
        System.out.print("Введіть слово: ");
        String word = scanner.nextLine();
        String[] translations = map.getTranslations(word);
        if (translations == null) {
            System.out.println("Переклад для слова '" + word + "' не знайдено.");
        } else {
            System.out.println("Переклад для слова '" + word + "':");
            System.out.println("Англійська: " + translations[0]);
            System.out.println("Японська: " + translations[1]);
            System.out.println("Німецька: " + translations[2]);
        }
    }
}