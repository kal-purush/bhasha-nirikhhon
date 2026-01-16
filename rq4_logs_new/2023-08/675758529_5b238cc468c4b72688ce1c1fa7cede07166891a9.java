package lesson6.creditCard;

/**
 Задача 1:
 Создать класс CreditCard c полями номер счета, текущая сумма на счету. Добавьте
 метод, который позволяет начислять сумму на кредитную карточку. Добавьте метод,
 который позволяет снимать с карточки некоторую сумму. Добавьте метод, который
 выводит текущую информацию о карточке. Напишите программу, которая создает три
 объекта класса CreditCard, у которых заданы номер счета и начальная сумма.
 Тестовый сценарий для проверки: Положите деньги на первые две карточки и снимите с
 третьей. Выведите на экран текущее состояние всех трех карточек
 */
public class CreditCard {
    String account_number;
    int sum;

    public CreditCard(String account_number,int sum){
        this.account_number = account_number;
        this.sum = sum;
    }

    public int accrual(int a){
        sum +=a;
        return sum ;
    }
    public int withdrawal(int a){
        return sum - a;
    }
    public void info(){
        System.out.println("Счёт " + account_number + " остаток на счёте " + sum);

    }

}