package lesson8.post;

/**
 Задача 1:
 Создать классы "Директор", "Рабочий", "Бухгалтер". Реализовать интерфейс с методом,
 который печатает название должности и имплементировать этот метод в созданные
 классы.
 */
public class Main {
    public static void main(String[] args) {
        Director director = new Director();
        director.printName();
        Accountant accountant = new Accountant();
        accountant.printName();
        Worker worker = new Worker();
        worker.printName();
    }
}