package com.kodilla.rps;

import java.util.Random;
import java.util.Scanner;

public class RpsRunner {
    public static void main(String[] args) {
        boolean end = false;
        Scanner scanner = new Scanner(System.in);
        String name;
        int rounds;
        String menu;
        int userMove;
        int computerMove;
        Random generator = new Random();
        int generalScore = 0;
        int roundScore = 0;
        while(!end) {
            System.out.println("Witemy w grze RPS");
            System.out.println(" n - nowa gra, x - wyjście z gry");
            menu = scanner.nextLine();

            switch (menu) {
                case "n" :
                    generalScore = 0;
                    roundScore = 0;
                    System.out.println("Podaj imie");
                    name = scanner.nextLine();
                    System.out.println("Podaj ilość rund");
                    rounds = Integer.parseInt(scanner.nextLine());
                    User user = new User(name, rounds);
                    System.out.println(user);
                    for(int i = 0; i < user.getRounds(); i++){
                        computerMove = (generator.nextInt(300) / 100) + 1;
                        System.out.println("computerMove " + computerMove);
                        System.out.println("Wybierz ruch: 1-kamień, 2-papier, 3-nożyczki");
                        userMove = Integer.parseInt(scanner.nextLine());
                        System.out.println("Twój ruch to: " + move(userMove));

                        System.out.println("Komputera ruch to: " + move(computerMove));
                        roundScore = 0;
                        roundScore = isWon(userMove, computerMove);
                        generalScore += roundScore;
                        if (roundScore > 0) System.out.println("Wygrana!");
                        if (roundScore == 0) System.out.println("Remis!");
                        if (roundScore < 0) System.out.println("Przegrana :(");
                    }
                    System.out.println("Końcowy wynik: " + generalScore);
                    break;
                case "x" :
                    end = true;
                    break;
            }

        }
    }

    public static String move(int k) {
        if (k == 1) return "kamień";
        if (k == 2) return "papier";
        if (k == 3) return "nożyczki";
        return "";
    }

    public static int isWon(int userMove, int computerMove) {
     //   int[userMove][computerMove]
        int[][] winningDiagram = new int[3][3];
        // result is 1=win , 0=draw, -1=lose
        winningDiagram[0][0] = 0;
        winningDiagram[0][1] = -1;
        winningDiagram[0][2] = 1;
        winningDiagram[1][0] = 1;
        winningDiagram[1][1] = 0;
        winningDiagram[1][2] = -1;
        winningDiagram[2][0] = -1;
        winningDiagram[2][1] = 1;
        winningDiagram[2][2] = 0;
        return winningDiagram[userMove-1][computerMove-1];
    }
}