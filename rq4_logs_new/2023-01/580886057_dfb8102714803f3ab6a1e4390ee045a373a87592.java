package pro.sky.java.course1.lesson4;

public class Program {
    public static void main(String[] args) {

        ////////// Task 1 & 2 //////////
        System.out.println("Task 1 & 2:");

        int clientOS = 1;
        int clientDeviceYear = 2015;

        boolean isIOS = clientOS == 0;
        boolean isAndroid = clientOS == 1;
        boolean isNewDevice = clientDeviceYear >= 2015;
        boolean isOldDevice = clientDeviceYear < 2015;

        if (isIOS && isOldDevice) {
            System.out.println("Установите облегченную версию приложения для iOS по ссылке");
        } else if (isIOS && isNewDevice) {
            System.out.println("Установите версию приложения для iOS по ссылке");
        } else if (isAndroid && isOldDevice) {
            System.out.println("Установите облегченную версию приложения для Android по ссылке");
        } else if (isAndroid && isNewDevice) {
            System.out.println("Установите версию приложения для Android по ссылке");
        }


        ////////// Task 3 //////////
        System.out.println("\nTask 3:");

        int year = 2000;

        if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) {
            System.out.println(year + " год является високосным");
        } else {
            System.out.println(year + " год не является високосным");
        }


        ////////// Task 4 //////////
        System.out.println("\nTask 4:");

        int deliveryDistance = 95;
        int deliveryTime = 1;

        if (deliveryDistance < 20) {
            System.out.println("Потребуется дней: " + deliveryTime);
        } else if (deliveryDistance < 60) {
            System.out.println("Потребуется дней: " + (deliveryTime + 1));
        } else if (deliveryDistance < 100) {
            System.out.println("Потребуется дней: " + (deliveryTime + 2));
        } else {
            System.out.println("Доставка по этому адресу невозможна");
        }


        ////////// Task 5 //////////
        System.out.println("\nTask 5:");

        int monthNumber = 12;

        switch (monthNumber) {
            case 1:
                System.out.println("Январь");
                break;
            case 2:
                System.out.println("Февраль");
                break;
            case 3:
                System.out.println("Март");
                break;
            case 4:
                System.out.println("Апрель");
                break;
            case 5:
                System.out.println("Май");
                break;
            case 6:
                System.out.println("Июнь");
                break;
            case 7:
                System.out.println("Июль");
                break;
            case 8:
                System.out.println("Август");
                break;
            case 9:
                System.out.println("Сентябрь");
                break;
            case 10:
                System.out.println("Октябрь");
                break;
            case 11:
                System.out.println("Ноябрь");
                break;
            case 12:
                System.out.println("Декабрь");
                break;
            default:
                System.out.println("Такого месяца не существует!");
        }
    }
}