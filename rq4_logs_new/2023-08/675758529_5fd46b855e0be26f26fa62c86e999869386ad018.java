package lesson6.creditCard;

public class Main {
    public static void main(String[] args) {
        CreditCard creditCard = new CreditCard("EWGWE12124", 213);
        CreditCard creditCard1 = new CreditCard("DFWEG124", 24124);
        CreditCard creditCard2 = new CreditCard("WEG1233", 14534);

        System.out.println("Карта " + creditCard.account_number + " сумма " + creditCard.sum);
        System.out.println("Cумма после начисления средств " + creditCard.accrual(10));

        System.out.println("Карта " + creditCard1.account_number + " сумма " + creditCard1.sum);
        System.out.println("Cумма после начисления средств " + creditCard1.accrual(10));

        System.out.println("Карта " + creditCard2.account_number + " сумма " + creditCard2.sum);
        System.out.println("Cумма после снятия средств " + creditCard1.withdrawal(10));

        creditCard.info();
        creditCard1.info();
        creditCard2.info();


    }
}