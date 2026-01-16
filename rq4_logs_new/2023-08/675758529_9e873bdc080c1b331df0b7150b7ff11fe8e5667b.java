package lesson6.ATM;

import java.util.Scanner;

/**
 * Задача *:
 * Создать класс, описывающий банкомат. Набор купюр, находящихся в банкомате, должен
 * задаваться тремя свойствами: количеством купюр номиналом 20, 50 и 100. Сделать
 * метод для добавления денег в банкомат. Сделать функцию, снимающую деньги, которая
 * принимает сумму денег, а возвращает булевое значение - успешность выполнения
 * операции. При снятии денег, функция должна распечатывать каким количеством купюр
 * какого номинала выдаётся сумма. Создать конструктор с тремя параметрами -
 * количеством купюр каждого номинала
 */
public class ATM {

    //колчичество купюр
    int sumTwenty = Logic.random(), sumFifty = Logic.random(), sumHungred = Logic.random();

    // сумма купюр ожного номинала
    int twenty = sumTwenty * 20, fifty = sumFifty * 50, hungred = sumHungred * 100;

    // все деньги
    int allMoney = twenty + fifty + hungred;


    public void addMoney() {

        System.out.println("Сколько вы внесли купюр номиналом 100?");
        sumHungred += Logic.logic();
        hungred = 100 * sumHungred;

        System.out.println("Сколько вы внесли купюр номиналом 50?");
        sumFifty += Logic.logic();
        fifty = 50 * sumFifty;

        System.out.println("Сколько вы внесли купюр номиналом 20?");
        sumTwenty += Logic.logic();
        twenty = 20 * sumTwenty;

        allMoney = twenty + fifty + hungred;

    }

    public void widrawMoney() {

        System.out.println("Cколько денег вы хотите снять ?");
        System.out.println("1: 20");
        System.out.println("2: 50");
        System.out.println("3: 100");
        System.out.println("4: Другая сумма");


        int x = Logic.logic();

        if (x > 5 && x < 0) {
            System.out.println("Введите число что-бы выполнить действие ");

        } else {

            switch (x) {
                case 1:
                    if (sumTwenty == 0) {
                        System.out.println("Нет купюр в банкомате");

                    } else {
                        System.out.println("Вы сняли двадцать рублей, заберите купюру ");
                        sumTwenty -= 1;
                        allMoney -= 20;
                        break;
                    }

                case 2:
                    if (sumFifty == 0) {
                        System.out.println("Нет купюр в банкомате");
                    } else {
                        System.out.println("Вы сняли пятьдесят рублей, заберите купюру ");
                        sumFifty -= 1;
                        allMoney -= 50;
                        break;
                    }

                case 3:
                    if (sumHungred > 0) {
                        System.out.println("Вы сняли сто рублей, заберите купюру ");
                        sumHungred -= 1;
                        allMoney -= 100;
                        break;
                    } else if (sumFifty >= 2) {
                        System.out.println("Вы сняли сто рублей, заберите купюры");
                        sumFifty -= 2;
                        allMoney -= 100;
                        break;
                    } else if (sumTwenty >= 5) {
                        System.out.println("Вы сняли сто рублей, заберите купюры");
                        sumTwenty -= 5;
                        allMoney -= 100;
                        break;
                    } else {
                        System.out.println("Нету купюр невозможно выдать столько денег");
                        break;
                    }
                case 4:
                    int c;

                    while (true) {

                        System.out.println("Введите сумму которую хотели бы снять ");
                        c = Logic.logic();

                        if (c % 10 != 0) {
                            System.out.println("Такую сумму не возможно снять");
                        } else if (c == 10 || c == 30) {
                            System.out.println("Такую сумму не возможно снять");
                        } else {
                            break;
                        }
                    }

                    if (c > sumHungred * 100 + sumFifty * 50 + sumTwenty * 20) {
                        System.out.println("Невозможно составить сумму с имеющимися купюрами.");
                    } else {
                        boolean isPossible = false;

                        for (int numTwenty = 0; numTwenty <= sumTwenty; numTwenty++) {
                            for (int numFifty = 0; numFifty <= sumFifty; numFifty++) {
                                for (int numHungred = 0; numHungred <= sumHungred; numHungred++) {

                                    if (numHungred * 100 + numFifty * 50 + numTwenty * 20 > c) {
                                        break;
                                    }

                                    if (numHungred * 100 + numFifty * 50 + numTwenty * 20 == c) {
                                        isPossible = true;
                                        System.out.println("Сумма составлена с помощью " + numHungred + " купюр по 100, " + numFifty + " купюр по 50 и " + numTwenty + " купюр по 20.");
                                        sumHungred -= numHungred;
                                        sumFifty -= numFifty;
                                        sumTwenty -= numTwenty;
                                        break;
                                    }
                                }
                                if (isPossible) {
                                    break;
                                }
                            }
                            if (isPossible) {
                                break;
                            }
                        }

                        if (!isPossible) {
                            System.out.println("Сумма не может быть составлена.");
                        }
                    }
            }
        }
    }

    public void info() {
        System.out.println("Количество всех денег " + allMoney);
        System.out.println("Количество купюр номиналома 20: " + sumTwenty);
        System.out.println("Количество купюр номиналома 50: " + sumFifty);
        System.out.println("Количество купюр номиналома 100: " + sumHungred);
        System.out.println();
        System.out.println("Нажмите Y что бы вернуться в главное меню");

        Scanner scanner = new Scanner(System.in);
        String userInput = "";

        while (!userInput.equals("y")) {
            System.out.println("Введите символ 'Y' для выхода: ");
            userInput = scanner.nextLine().toLowerCase();
        }

        System.out.println("Вы ввели символ 'Y'. Вы вышли в главное меню.");
        System.out.println();
    }
}