package src.main.java.homework6;

public class Main {

    public static void main(String[] args) {

        printThreeWords();
        checkSumSign();
        printColor();
        compareNumbers();
        compareNumbersBool(10, 12);
        compareNumbersBool(1, 2);
        compareNumbersBool(10, 0);
        compareNumbersBool(10, 5);
        compareNumbersBool(10, 10);
        isPositiveNumber(-2);
        isPositiveNumber(0);
        isPositiveNumber(32);
        printString("Homework is done!", 5);
        isPositiveNumberBool(-1);
        isPositiveNumberBool(0);
        isPositiveNumberBool(3);
        leapYearBool(2028);
        leapYearBool(4);
        leapYearBool(100);
        leapYearBool(400);
        leapYearBool(600);


    }

    static void printThreeWords() {
        System.out.println("\nOrange.\nBanana.\nApple.");
    }

    static void checkSumSign() {
        int a = -150;
        int b = 100;

        if (a + b >= 0) {
            System.out.println("\nSum is positive: " + (a + b));
        } else {
            System.out.println("\nSum is negative: " + (a + b));
        }
    }


    static void printColor() {
        int a = 101001001;

        if (a <= 0) {
            System.out.println("\nЧервоний");
        } else if (a > 0 && a <= 100) {
            System.out.println("\nЖовтий");
        } else {
            System.out.println("\nЗелений");
        }
    }

    static void compareNumbers() {
        int a = 32;
        int b = 21;
        if (a >= b) {
            System.out.println("\na >= b");
        } else {
            System.out.println("\na < b");
        }
    }

    static boolean compareNumbersBool(int a, int b) {

        if ((a + b) <= 20 && (a + b) >= 10) {
            return true;
        }
        return false;
    }

    static void isPositiveNumber(int a) {

        if (a >= 0) {
            System.out.println("\nДодатнє число");
        } else {
            System.out.println("\nВід’ємне число");
        }
    }

    static boolean isPositiveNumberBool(int a) {
        if (a >= 0) {
            return false;
        }
        return true;

    }

    static void printString(String string, int a) {
        System.out.println();
        for (int i = 0; i < a; i++) {
            System.out.println(string);
        }
    }

    static boolean leapYearBool(int year) {

//  1st realization, too much code; keep it fore future educational references
//        if (year %400 == 0){
//            return true;
//        }else if (year %4 == 0 && year %100 > 0){
//            return true;
//        }else {
//            return false;
//        }

        if (year % 4 == 0) {
            if (year % 100 == 0 && year % 400 > 0) {
                return false;
            }
            return true;
        }
        return false;
    }


}