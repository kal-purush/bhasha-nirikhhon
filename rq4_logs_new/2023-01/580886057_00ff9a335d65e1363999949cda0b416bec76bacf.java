package pro.sky.java.course1.additional.tasks2;

public class Program {
    public static void main(String[] args) {

        /////////// Task 6 //////////
        System.out.println("Task 6:");

        int age = 19;
        int salary = 58_000;
        int limit = 0;
        if (age >= 23) {
            limit = 3 * salary;
        } else {
            limit = 2 * salary;
        }
        if (salary >= 80_000) {
            limit *= 1.5;
        } else if (salary >= 50_000) {
            limit *= 1.2;
        }
        System.out.println("Мы готовы выдать вам кредитную карту с лимитом " + limit + " рублей");


        /////////// Task 7 //////////
        System.out.println("\nTask 7:");

        age = 25;
        salary = 60_000;
        int wantedSum = 330_000;
        double basicRate = 10;
        int creditPeriod = 12;
        int maxAcceptablePayment = salary / 2;
        if (age < 23) {
            basicRate = basicRate + 1;
        } else if (age < 30) {
            basicRate = basicRate + 0.5;
        }
        if (salary > 80_000) {
            basicRate = basicRate - 0.7;
        }
        double monthlyCreditPayment = wantedSum * (1 + basicRate/100) / creditPeriod;
        System.out.print("Максимальный платеж при ЗП " + salary + " равен " + maxAcceptablePayment + " рублей. Платеж по кредиту " + monthlyCreditPayment + " рублей.");
        String s = (maxAcceptablePayment > monthlyCreditPayment) ? " Одобрено!" : " Отказано.";
        System.out.println(s);
    }
}