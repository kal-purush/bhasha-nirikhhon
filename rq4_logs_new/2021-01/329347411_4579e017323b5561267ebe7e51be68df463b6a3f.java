package homeWork.firstLesson;

import org.w3c.dom.ls.LSOutput;

public class Main {
    public static void main(String[] args) {
        // Problem 2
        boolean student = true;

        byte age = 24;
        short pinCode = 1234;
        int carsNumber = 12345;
        long starsCounter = 123456L;

        double salary = 1350.30;
        float floatPoint = 12.3F;

        String name = "Vladislav";
        char firstNameLetter = 'V';
        // =========================

        System.out.println(mathematicsCalculation(10.0F, 20.0F, 60.0F, 30.0F));
        System.out.println(control10Diapason20(10,5));
        positiveControlNumber(15);
        System.out.println(negativeControlNumber(-5));
        helloName("Vladislav");
        checkLeapYear(404);

    }

    /* Problem 3 */
    static float mathematicsCalculation(float a, float b, float c, float d) {
        return a * (b + (c / d));
    }

    /* Problem 4 */
    static boolean control10Diapason20(int a , int b) {
        return a + b >= 10 && a + b <= 20;
    }

    /* Problem 5 */
    static void positiveControlNumber(int number) {
        if(number >= 0) {
            System.out.println("положительное число");
        } else {
            System.out.println("отрицательное число");
        }
    }

    /* Problem 6 */
    static boolean negativeControlNumber(int number) {
        return !(number >= 0);
    }

    /* Problem 7 */
    static void helloName (String name) {
        System.out.println("Привет, " + name + "!");
    }

    /* Problem 8 */
    static void  checkLeapYear(int year) {
        if (year % 4 != 0 || (year % 100 == 0 && year % 400 != 0)) {
            System.out.println("не високосный");
        } else {
            System.out.println("високосный");
        }
    }
}