package com.adventofcode.challenge;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/* Day 1 :
* On each line, the calibration value can be found by combining the first digit and the last digit (in that order) to form a single two-digit number.
* The final result will be obtained by adding together all the two-digit numbers.
* */

public class Main {
    public static void main(String[] args) {
        // Toutes ces syntaxes sont correctes :
        //String nom_fichier = "C:\\Users\\Martine\\Documents\\IntelliJ\\AdventOfCode\\src\\main\\resources\\data.txt";
        //String nom_fichier = "src\\main\\resources\\data.txt";
        //String nom_fichier = "src/main/resources/data.txt";
        String nom_fichier = "src/main/resources/input_trebuchet.txt";

        //Scanner scanner = new Scanner(System.in);
        try (Scanner scanner = new Scanner(new File(nom_fichier))) {

            /* Day 1 - Part One :
            int somme = 0;
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                boolean chiffre = false;
                int first = 0, last = 0;
                for(int i = 0; i < line.length(); i++) {
                    String caractere = String.valueOf(line.charAt(i));
                    if(caractere.matches("[0-9]") && chiffre == false) {
                        first = Integer.parseInt(caractere);
                        last = Integer.parseInt(caractere);
                        chiffre = true;
                    } else if(caractere.matches("[0-9]")) {
                        last = Integer.parseInt(caractere);
                    }
                }
                String number = "0" + first + last;
                int twodigits = Integer.parseInt(number);
                somme += twodigits;
            }
            System.out.println(somme);

            // => 54630
            // => That's the right answer! You are one gold star closer to restoring snow operations [Continue to Part Two]
            */

            // Part Two : some digits are spelled out with letters, and one, two, three, four, five, six, seven, eight, and nine also count as "digits".
            int somme = 0;
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                boolean chiffre = false;
                int first = 0, last = 0;

                for(int i = 0; i < line.length(); i++) {
                    String caractere = String.valueOf(line.charAt(i));

                    int oldlast = last;
                    last = switch(caractere) {
                        case "o" -> {   // one
                            if(i+2 < line.length() && String.valueOf(line.charAt(i+1)).equals("n") && String.valueOf(line.charAt(i+2)).equals("e")) {
                                yield 1;
                            } else {
                                yield oldlast;
                            }
                        }
                        case "t" -> {   // two or three
                            if (i+2 < line.length() && String.valueOf(line.charAt(i+1)).equals("w") && String.valueOf(line.charAt(i+2)).equals("o")) {
                                yield 2;
                            } else if (i+4 < line.length() && String.valueOf(line.charAt(i+1)).equals("h") && String.valueOf(line.charAt(i+2)).equals("r")
                                    && String.valueOf(line.charAt(i+3)).equals("e") && String.valueOf(line.charAt(i+4)).equals("e")) {
                                yield 3;
                            } else {
                                yield oldlast;
                            }
                        }
                        case "f" -> {   // four or five
                            if (i+3 < line.length() && String.valueOf(line.charAt(i+1)).equals("o") && String.valueOf(line.charAt(i+2)).equals("u")
                                    && String.valueOf(line.charAt(i+3)).equals("r")) {
                                yield 4;
                            } else if (i+3 < line.length() && String.valueOf(line.charAt(i+1)).equals("i") && String.valueOf(line.charAt(i+2)).equals("v")
                                    && String.valueOf(line.charAt(i+3)).equals("e")) {
                                yield 5;
                            } else {
                                yield oldlast;
                            }
                        }
                        case "s" -> {   // six or seven
                            if (i+2 < line.length() && String.valueOf(line.charAt(i+1)).equals("i") && String.valueOf(line.charAt(i+2)).equals("x")) {
                                yield 6;
                            } else if (i+4 < line.length() && String.valueOf(line.charAt(i+1)).equals("e") && String.valueOf(line.charAt(i+2)).equals("v")
                                    && String.valueOf(line.charAt(i+3)).equals("e") && String.valueOf(line.charAt(i+4)).equals("n")) {
                                yield 7;
                            } else {
                                yield oldlast;
                            }
                        }
                        case "e" -> {   // eight
                            if (i+4 < line.length() && String.valueOf(line.charAt(i+1)).equals("i") && String.valueOf(line.charAt(i+2)).equals("g")
                                    && String.valueOf(line.charAt(i+3)).equals("h") && String.valueOf(line.charAt(i+4)).equals("t")) {
                                yield 8;
                            } else {
                                yield oldlast;
                            }
                        }
                        case "n" -> {   // nine
                            if (i+3 < line.length() && String.valueOf(line.charAt(i+1)).equals("i") && String.valueOf(line.charAt(i+2)).equals("n")
                                    && String.valueOf(line.charAt(i+3)).equals("e")) {
                                yield 9;
                            } else {
                                yield oldlast;
                            }
                        }
                        default -> {
                            if(caractere.matches("[0-9]")) {
                                yield Integer.parseInt(caractere);
                            } else {
                                yield oldlast;
                            }
                        }
                    };
                    if(chiffre == false && last != 0) {
                        first = last;
                        chiffre = true;
                    }
                }
                String number = "0" + first + last;
                int twodigits = Integer.parseInt(number);
                somme += twodigits;
            }
            System.out.println(somme);

            // => 54770
            // => That's the right answer! You are one gold star closer to restoring snow operations. You have completed Day 1!

        } catch(FileNotFoundException e) {
            System.out.println("Aucun fichier à lire");
        }
    }
}