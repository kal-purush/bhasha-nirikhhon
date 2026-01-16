package com;

import java.util.List;

abstract public class PrintMenu {
    public static void main() {
        System.out.println("----------------------------------------");
        System.out.println("Välkommen till Javabanken!");
        System.out.println("----------------------------------------");
        System.out.println("1. Sök kund");
        System.out.println("2. Skapa kund");
        System.out.println("3. Personal");
        System.out.println("0. Avsluta\n");
    }

    public static void search() {
        System.out.println("----------------------------------------");
        System.out.println("Javabanken > Sök kund");
        System.out.println("----------------------------------------");
        System.out.println("1. Sök namn");
        System.out.println("2. Sök personnummer");
        System.out.println("0. Avbryt\n");
    }

    public static void customerOptions() {
        System.out.println("----------------------------------------");
        System.out.println("Javabanken > Kundalternativ");
        System.out.println("----------------------------------------");
        System.out.println("1. Visa Kundinformation");
        System.out.println("2. Skapa nytt konto");
        System.out.println("3. Redigera personlig information");
        System.out.println("4. Redigera konto information/ Gör en överföring"); // return list of accounts.
        System.out.println("0. Tillbaka\n");
    }

    public static void editCustomer() {
        System.out.println("----------------------------------------");
        System.out.println("Javabanken > Redigera kund");
        System.out.println("----------------------------------------");
        System.out.println("1. Redigera förnamn");
        System.out.println("2. Redigera efternamn");
        System.out.println("3. Redigera email");
        System.out.println("0. Tillbaka\n");
    }

    public static void accountOptions() {
        System.out.println("----------------------------------------");
        System.out.println("Javabanken > Kontoalternativ");
        System.out.println("----------------------------------------");
        System.out.println("1. Visa Kontoinformation");
        System.out.println("2. Redigera saldo");
        System.out.println("3. Redigera skuld");
        System.out.println("4. Gör en överföring");
        System.out.println("0. Tillbaka\n");
    }

    public static void searchResult(List<String> searchResults) {
        int index = 0;
        System.out.println("----------------------------------------");
        System.out.println("Javabanken > Sökresultat");
        System.out.println("----------------------------------------");
        for(String path:searchResults) {
            System.out.println(++index+". "+path);
        }
        System.out.println("0. Tillbaka\n");
    }
}