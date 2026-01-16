package com.company.Exams;
import java.util.Scanner;
import java.text.*;

public class Battles {
    public static void main(String[] args) {
        Scanner console = new Scanner(System.in);

        // Ако бъде достигнaт максималният брой битки, програмата трябва да приключи.

        int pokemonsFirstPlayer = Integer.parseInt(console.nextLine());
        int pokemonsSecondPlayer = Integer.parseInt(console.nextLine());
        int maxFights = Integer.parseInt(console.nextLine());
        int battles = 0;

        for (int i = 1; i <= pokemonsFirstPlayer; i++) {

            for (int k = 1; k <= pokemonsSecondPlayer; k++) {

                battles++;
                if (battles > maxFights) {
                    break;
                }

                System.out.printf("(" + i + " <-> " + k + ") ");
            }
        }
    }
}