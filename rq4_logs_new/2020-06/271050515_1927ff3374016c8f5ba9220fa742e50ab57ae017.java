package com.kodilla.rps;

import java.util.Scanner;
import java.util.regex.Pattern;

public class DataLoader {
    Scanner scanner = new Scanner(System.in);

    public User getUserData() {
        System.out.println("Podaj imie: ");
        return new User(scanner.nextLine());
    }

    public Game getGameData() {
        boolean dataOK = false;
        String scannerInput;
        int rounds = 0;
        int joker = -1;
        int difficulty = 0;
        while (!dataOK) {
            System.out.println("Podaj ilość rund: (>0)");
            scannerInput = scanner.nextLine();
            if (isNumeric(scannerInput) && Integer.parseInt(scannerInput) > 0) {
                rounds = Integer.parseInt(scannerInput);
                dataOK = true;
            }
        }
        dataOK = false;
        while (!dataOK) {
            System.out.println("Joker umożliwia powtórne rozegranie rundy. Podaj ilość jokerów: (>=0)");
            scannerInput = scanner.nextLine();
            if (isNumeric(scannerInput) && Integer.parseInt(scannerInput) >= 0) {
                joker = Integer.parseInt(scannerInput);
                dataOK = true;
            }
        }
        dataOK = false;
        while (!dataOK) {
            System.out.println("Wybierz trudność: 1 - normal, 2 - hard");
            scannerInput = scanner.nextLine();
            if (isNumeric(scannerInput)
                    && (Integer.parseInt(scannerInput) == 1
                            || Integer.parseInt(scannerInput) == 2
                            || Integer.parseInt(scannerInput) == 3)) {
                difficulty = Integer.parseInt(scannerInput);
                dataOK = true;
            }
        }
        return new Game(rounds, joker, difficulty);
    }

    public static boolean isNumeric(String strNum) {
        final Pattern pattern = Pattern.compile("-?\\d+");
        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }
}