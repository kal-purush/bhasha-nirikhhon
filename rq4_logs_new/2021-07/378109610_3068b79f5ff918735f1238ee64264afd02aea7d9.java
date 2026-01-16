import java.util.Random;
import java.util.Scanner;

public class Task3 {
    public static void main(String[] args) {
        int odd = calculateCountOfOddElementsInMatrix(new int[]{0, -2, 3, -1, 5, 10});
        System.out.println("Задача 3: Колличество нечетных элементов в массиве : " + odd);

        int number = operation(0);
        System.out.println("Задача 2: Номер в зависимости от условия: " + number);

        System.out.println("Задача 4: Склонение в зависимости от числа");
        for (int i = 0; i <= 1000; i++) {
            countDevs(i);
            System.out.println();
        }

        printArray();
        System.out.println();

        foobar(6);
        foobar(10);
        foobar(15);

        calculateSumOfDiagonalElements(3);

        printMatrix(2, 3);
        System.out.println();

        System.out.println("Задача 8: Вывод простых чисел");
        printPrimeNumbers(1000);
    }

    /**
     * Необходимо прочитать с консоли значение числа типа int,
     * далее создать одноменрый массив типа int размера прочитаного с консоли
     * далее заполнить массив случайными значениями
     * далее вывести массив на консоль
     */
    private static void printArray() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Введите размер массива по задаче 1: ");
        int size = scanner.nextInt();
        int[] myArray = new int[size];
        Random random = new Random();
        for (int i = 0; i < myArray.length; i++) {
            myArray[i] = random.nextInt();
        }
        System.out.println("\nЗадача 1: Вывод элементов массива myArray: ");
        for (int i = 0; i < myArray.length; i++) {
            System.out.print(myArray[i] + " ");
        }
    }

    /**
     * Метод должен выполнять некоторую операцию с int "number" в зависимости от некоторых условий:
     * - if number положительное число, то необходимо number увеличить на 1
     * - if number отрицательное - уменьшить на 2
     * - if number равно 0 , то замените значение number на 10
     * вернуть number после выполнения операций
     */

    private static int operation(int number) {
        if (number > 0) {
            number = number + 1;
        } else if (number < 0) {
            number = number - 2;
        } else {
            number = 10;
        }
        return number;
    }

    /**
     * На вход приходит массив целых чисел типа int
     * Необходимо найти количество нечетных элементов в массиве и вернуть значение в метод main,
     * в котором это значение распечатается на консоль.
     */
    private static int calculateCountOfOddElementsInMatrix(int[] array) {
        int odd = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] % 2 != 0) {
                odd++;
            }
        }
        return odd;
    }

    /**
     * На вход приходит число.
     * Вывести в консоль фразу из разряда "_COUNT_ программистов",
     * заменить _COUNT_ на число которое пришло на вход в метод и заменить окончание в слове "программистов" на
     * уместное с точки зрения русского языка.
     * Пример: 1 программист, 42 программиста, 50 программистов
     */
    public static void countDevs(int count) {
//        System.out.print("Введите любое целое число: ");
//        Scanner scanner = new Scanner(System.in);
//        count = scanner.nextInt();
        int с10 = count % 10;
        int c100 = count % 100;
        String word = "";
        if (c100 >= 11 && c100 <= 19) {
            word = "Программистов";
        } else if (с10 >= 2 && с10 <= 4) {
            word = "Программиста";
        } else if (с10 == 1) {
            word = "Программист";
        } else {
            word = "Программистов";
        }
        System.out.printf("%d %s", count, word);
    }

    /**
     * Метод должен выводить разные строки в консоли в зависимости от некоторых условий:
     * - если остаток от деления на 3 равен нулю - выведите "foo" (example of number - 6)
     * - если остаток от деления на 5 равен нулю - вывести "bar" (example of number - 10)
     * - если остаток от деления на 3 и 5 равен нулю 0 ,то вывести "foobar" (example of number - 15)
     */
    public static void foobar(int number) {
        String someWord = "";
        if (number % 3 == 0) {
            someWord = "foo";
        }
        if (number % 5 == 0) {
            someWord = "bar";
        }
        if ((number % 3 == 0) && (number % 5 == 0)) {
            someWord = "foobar";
        }
        System.out.println(someWord);
    }

    /**
     * заполнить рандомно 2-х мерный массив и посчитать сумму элементов на диагонали
     */
    public static int calculateSumOfDiagonalElements(int rows) {
        int summa = 0;
        int[][] myArray = new int[rows][rows];
        Random random = new Random();
        for (int i = 0; i < myArray.length; i++) {
            for (int j = 0; j < myArray[i].length; j++) {
                myArray[i][j] = random.nextInt();
            }
        }
        System.out.println("Сгенерированная матрица по задаче 6: ");
        for (int i = 0; i < myArray.length; i++, System.out.println()) {
            for (int j = 0; j < myArray[i].length; j++) {
                System.out.print(myArray[i][j] + " ");
            }
        }
        for (int i = 0; i < myArray.length; i++) {
            for (int j = 0; j < myArray[i].length; j++) {
                if (i == j) {
                    summa += myArray[i][j];
                }
            }
        }
        System.out.println("Сумма элементов по диагонали матрицы по задаче 6 = " + summa);
        return summa;
    }

    /**
     * Шаги по реализации:
     * - Прочитать два int из консоли
     * - Создайте двумерный массив int (используйте целые числа, которые вы читаете по высоте и ширине консоли)
     * - Заполнить массив случайными значениями (до 100)
     * - Вывести в консоль матрицу заданного размера, но:
     * - Если остаток от деления элемента массива на 3 равен нулю - выведите знак "+" вместо значения элемента массива.
     * - Если остаток от деления элемента массива на 7 равен нулю - выведите знак "-" вместо значения элемента массива.
     * - В противном случае выведите "*"
     * <p>
     * Example:
     * - Значения с консоли - 2 и 3
     * - Массив будет выглядеть так (значения будут разными, потому что он случайный)
     * 6 11 123
     * 1 14 21
     * - Для этого значения вывод в консоли должен быть:
     * <p>
     * + * *
     * * - +
     * <p>
     * Обратите внимание, что 21% 3 == 0 и 21% 7 = 0, но выводить надо не +-, а +
     */
    public static void printMatrix(int size1, int size2) {
        int[][] matrix = new int[size1][size2];
        Random random = new Random();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                matrix[i][j] = random.nextInt(100);
            }
        }
        System.out.println("Сгенерированная матрица по задаче 7: ");
        for (int i = 0; i < matrix.length; i++, System.out.println()) {
            for (int j = 0; j < matrix[i].length; j++) {
                System.out.print(matrix[i][j] + " ");
            }
        }
        for (int i = 0; i < matrix.length; i++, System.out.println()) {
            for (int j = 0; j < matrix[i].length; j++) {
                if ((matrix[i][j]) % 3 == 0) {
                    System.out.print("+ ");
                } else if ((matrix[i][j]) % 7 == 0) {
                    System.out.print("- ");
                } else {
                    System.out.print("* ");
                }
            }
        }
    }

    /**
     * Задача со звездочкой!
     * Метод должен печатать все простые числа <1000
     * что такое просто число (https://www.webmath.ru/poleznoe/formules_18_5.php)
     */
    public static int printPrimeNumbers(int limitForNumber) {
        for (int i = 2; i <= limitForNumber; i++) {
            Integer number = i;
            for (int j = 2; j < i; j++) {
                if (i % j == 0) {
                    number = null;
                    break;
                }
            }
            if (number != null) {
                System.out.println(number);
            }
        }
        return limitForNumber;
    }
}


