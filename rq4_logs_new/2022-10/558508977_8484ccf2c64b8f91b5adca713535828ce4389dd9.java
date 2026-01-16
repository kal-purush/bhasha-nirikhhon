public class Main {
    public static void main(String[] args) {


        ///Task 1


        int clientOS = 1;

        if(clientOS == 0){

            System.out.println("Установите версию приложения для iOS по ссылке.");
        } else {System.out.println("Установите версию приложения для Android по ссылке.");}


        ///Task 2


        int clientOS1 = 1;

        int clientDeviceYear = 2017;

        if(clientOS == 0 && clientDeviceYear < 2015) {
            System.out.println("Установите облегченную версию приложения" +
                    " для iOS по ссылке.");

        } else if (clientOS == 1 && clientDeviceYear < 2015) {
            System.out.println("Установите облегченную версию приложения" +
                    " для Android по ссылке.");
        }

        if(clientOS == 0 && clientDeviceYear >= 2015) {
            System.out.println("Установите версию приложения для iOS по ссылке.");

        } else if (clientOS == 1 && clientDeviceYear >= 2015) {
            System.out.println("Установите версию приложения для Android по ссылке.");


        }


        ///Task 3


        int year = 800;
        int yearFour = year % 4;
        int yearHund = year % 100;
        int yearQuart = year % 400;

        if((yearFour == 0 && yearHund != 0) || yearQuart == 0) {

            System.out.println("Год весокосный.");

        } else if(yearFour != 0 || (yearHund == 0 && yearQuart != 0)) {
            System.out.println("Год невесоковсный.");

        }


        ///Task 4**
        ///Решение основано на решении упрощённой аналогичной задачи.


        int deliveryDistance = 60;
        int deliveryDistanceAdj = deliveryDistance - 20;
        int distanceBy40 = deliveryDistanceAdj / 40;
        int days = distanceBy40 + 2;

        if(deliveryDistance < 20) {
            System.out.println("Потребуется дней: 1");
        } else {
            System.out.println("Потребуется дней:" + days);

        }


        ///Task 5


        int monthNumber = 15;

        switch(monthNumber) {

            case 1:
            case 2:
            case 12:
                System.out.println("Зима.");
                break;

            case 3:
            case 4:
            case 5:
                System.out.println("Весна.");
                break;


            case 6:
            case 7:
            case 8:
                System.out.println("Лето.");
                break;

            case 9:
            case 10:
            case 11:
                System.out.println("Осень.");
                break;

            default:
                System.out.println("Такого месяца не существует.");

        }








    }
}