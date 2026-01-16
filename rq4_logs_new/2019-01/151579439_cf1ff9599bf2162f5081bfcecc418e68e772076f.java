package tasks.interviews;

/**
 * Написать программу, которая выводит 4
 * последовательность первых n чисел Фибоначч.
 *
 * @author Aleksei Sapozhnikov (vermucht@gmail.com)
 * @version 0.1
 * @since 0.1
 */
public class Fibonacci {

    /**
     * Последовательность чисел Фибоначчи.
     *
     * @param n Сколько чисел брать.
     * @return Массив с числами.
     */
    public int[] firstN(int n) {
        int[] numbers = new int[n];
        numbers[0] = 1;
        numbers[1] = 1;
        for (int i = 2; i < n; i++) {
            numbers[i] = numbers[i - 1] + numbers[i - 2];
        }
        return numbers;
    }

    /**
     * Число Фибоначчи с данным номером.
     *
     * @param n Номер числа.
     * @return Число.
     */
    public int numberN(int n) {
        int a = 1;
        int b = 1;
        int next;
        for (int i = 2; i <= n; i++) {
            next = a + b;
            a = b;
            b = next;
        }
        return a;
    }

    /**
     * Рекурсивный способ НЕ РЕКОМЕНДУЕТСЯ, поскольку
     * при вызове, например, f(5) будет вызван
     * f(4), f(3), f(2) и f(1) - многократное повторение
     * одних и тех же операций.
     *
     * @param n Номер числа Фибоначчи.
     * @return Число Фибоначчи с номером n.
     */
    public int numberNRecursive(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        return numberNRecursive(n - 1) + numberNRecursive(n - 2);
    }
}