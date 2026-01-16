
package ru.geekbrains.lesson1;

public class FirstApp {
    public static void main(String[] args) {
        boolean valTestBool     = false;
        char valTestChar        = 'c';
        byte valTestByte        = 13;
        short valTestShort      = 13;
        int valTestInt          = 13;
        long valTestLong        = 13L;
        float valTestFloat      = 13.0f;
        double valTestDouble    = 13.0;

        System.out.println(executionTask(10, 13, 21, 32));
        System.out.println(checkSum(2, 6));
        checkSign(0);
        System.out.println(invertedSignCheck(-18));
        helloName("Михаил");
        checkYear(396);
    }

    // Математическое выражение.
    static float executionTask(float a, float b, float c, float d) {
        return a * (b + (c / d));
    }

    // Проверка вхождения суммы аргументов в интервал [10; 20].
    static boolean checkSum(int inputValue1, int inputValue2)    {
        return 10 <= (inputValue1 + inputValue2) && 20 >= (inputValue1 + inputValue2);
    }

    // Проверка знака числа, вывод ответа в консоль.
    static void checkSign(int intputValue)    {
        if(intputValue >= 0)    {
            System.out.println("Положительное число.");
            return;
        }
        System.out.println("Отрицательное число.");
    }

    // Проверка знака чиcла, инвертированная логика в ответе.
    static boolean invertedSignCheck(int inputValue1)    {
        return !(inputValue1 >= 0);
    }

    // Конкатенация строк. Приветствие.
    static void helloName(String inputName)    {
        System.out.println("Привет, " + inputName + "!");
    }

    // Високосный год.
    static void checkYear(int inputYear)    {
        if (inputYear % 400 == 0)   {
            // сразу на входе проверяем, что год - високосный.
            System.out.println("Год " + inputYear + " - високосный.");
            return;
        }
        else if (inputYear % 100 == 0)  {
            // этот - точно не високосный.
            System.out.println("Год " + inputYear + " - не високосный.");
            return;
        }
        else if (inputYear % 4 == 0)  {
            // год - високосный.
            System.out.println("Год " + inputYear + " - високосный.");
            return;
        }

        // ни одно из условий не прошло, значит год - не високосный.
        System.out.println("Год " + inputYear + " - не високосный.");
    }
}