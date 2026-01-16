public class Main {
    public static void main(String[] args) {
//        Домашняя работа 2.1
//        Задание 1
        System.out.println("Задание 1");
        double contribution = 15_000; // Вклад
        double accumulationAmount = 0; // Cумма накопления
        int i = 0;
        while (accumulationAmount <= 2_459_000) {
            accumulationAmount = accumulationAmount + accumulationAmount / 100;
            accumulationAmount = accumulationAmount + contribution;
            i += 1;
            System.out.println("Месяц " + i + " сумма накоплений равна " + String.format("%.2f", accumulationAmount) + " рублей");
            }
            System.out.println(i + " месяцев, необходимо для накопления данной суммы");
    }
}