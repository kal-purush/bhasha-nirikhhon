package pro.sky.java.course1.lesson6;

public class Program {
    public static void main(String[] args) {

        ////////// HOMEWORK 1 //////////
        System.out.println("HOMEWORK 1");

        ////////// Task 1 //////////
        System.out.println("\nTask 1:");

        int monthlySaving = 15_000;
        int totalSum = 0;
        int monthNumber = 1;
        while (totalSum < 2_459_000) {
            totalSum += totalSum / 100;
            totalSum += monthlySaving;
            System.out.println("Месяц " + monthNumber + ", сумма накоплений равна " + totalSum + " рублей");
            monthNumber++;
        }


        ////////// Task 2 //////////
        System.out.println("\n\nTask 2:");

        int i = 1;
        while (i <= 10) {
            System.out.print(i + " ");
            i++;
        }
        System.out.println();
        for (int j = 10; j >= 1; j--) {
            System.out.print(j + " ");
        }


        ////////// Task 3 //////////
        System.out.println("\n\n\nTask 3:");

        int population = 12_000_000;
        int birthRateAYearPerThousandPeople = 17;
        int mortalityRateAYearPerThousandPeople = 8;
        int yearNumber = 1;
        while (yearNumber <= 10) {
            population += (population / 1000) * (birthRateAYearPerThousandPeople - mortalityRateAYearPerThousandPeople);
            System.out.println("Год " + yearNumber + ", численность населения составляет " + population);
            yearNumber++;
        }
    }
}