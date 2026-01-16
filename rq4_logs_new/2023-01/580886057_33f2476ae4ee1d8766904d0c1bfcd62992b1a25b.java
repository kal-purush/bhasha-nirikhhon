package pro.sky.java.course1.lesson3;

public class Program {
    public static void main(String[] args) {

        ////////// HOMEWORK 1 //////////
        System.out.println("HOMEWORK 1");

        ////////// Task 1 //////////
        System.out.println("\nTask 1:");

        int age = 20;
        boolean isMajority = age >= 18;
        boolean isNotMajority = age < 18;
        if (isMajority) {
            System.out.println("Поздравляем вас с тем, что вы полностью совершеннолетний человек!");
        }
        if (isNotMajority) {
            System.out.println("Вынуждены вас огорчить, но на данный момент вы не достигли возраста совершеннолетия. Нужно ещё немного подождать...");
        }


        ////////// Task 2 //////////
        System.out.println("\nTask 2:");

        // Переменная age инициализирована в Задаче 1 Домашнего задания - 1
        if (age >= 7 && age < 18) {
            System.out.println("Вы ходите в школу");
        }
        if (age >= 18 && age < 24) {
            System.out.println("Вы закончили школу и можете поступить в университет");
        }
        if (age >= 24) {
            System.out.println("Вы окончили университет и пора бы отправиться на поиски первой работы");
        }


        ////////// Task 3 //////////
        System.out.println("\nTask 3:");

        int capacity = 102;
        int sittingSeats = 60;
        int standingSeats = capacity - sittingSeats;

        int sittingSeatsUsed = 60;
        int standingSeatsUsed = 15;

        if (sittingSeatsUsed < sittingSeats) {
            System.out.println("Сидячие места есть");
        }
        if (sittingSeatsUsed == sittingSeats) {
            System.out.println("Сидячих мест нет");
        }
        if (standingSeatsUsed < standingSeats) {
            System.out.println("Стоячие места есть");
        }
        if (standingSeatsUsed == standingSeats) {
            System.out.println("Стоячих мест нет");
        }
    }
}