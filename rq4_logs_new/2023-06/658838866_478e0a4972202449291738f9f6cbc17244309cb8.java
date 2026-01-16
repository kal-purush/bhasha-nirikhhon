package test;
/*ZADANIE:Vytvorte triedu Obdlznik, v ktorej budú 2 statické metódy. Prvá na výpočet obvodu obdĺžnika (vypocitajObvod), druhá na výpočet jeho obsahu (vypocitajObsah).Vytvorte jUnit testy, ktore otestuju funkcnost metod vypocitajObvod a vypocitajObsah*/
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.InputMismatchException;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Obdlznik {
    public static double vypocitajObvod(double stranaA, double stranaB) {
        return 2 * (stranaA + stranaB);
    }
    public static double vypocitajObsah(double stranaA, double stranaB) {
        return stranaA * stranaB;
    }

    public static void main(String[] args) {
    Scanner scn=new Scanner(System.in);
    double a=2.5;
    double b=3.5;
//    try {
//        System.out.println("Zadaj stranu a");
//        a=scn.nextDouble();
//        System.out.println("Zadaj stranu b");
//        b=scn.nextDouble();
//
//    } catch (InputMismatchException e) {
//
//    }
        System.out.println("Obvod obldznika je: "+Obdlznik.vypocitajObvod(a,b));
        System.out.println("Obsah obldznika je: "+Obdlznik.vypocitajObsah(a,b));
}

    @Test
    @DisplayName("Testy na vypocet obvodu")
    public void testVypocitajObvod() {
        assertEquals(10, Obdlznik.vypocitajObvod(2, 3));
        assertEquals(13, Obdlznik.vypocitajObvod(2.5, 4));
        }
        }