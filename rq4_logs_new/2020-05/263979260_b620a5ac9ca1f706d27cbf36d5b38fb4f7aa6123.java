public class MainClass {
//HW1
/*1. Создать пустой проект в IntelliJ IDEA и прописать метод main();
2. Создать переменные всех пройденных типов данных, и инициализировать их значения;
3. Написать метод вычисляющий выражение a * (b + (c / d)) и возвращающий результат,где a, b, c, d – входные параметры этого метода;
4. Написать метод, принимающий на вход два числа, и проверяющий что их сумма лежит в пределах от 10 до 20(включительно), если да – вернуть true, в противном случае – false;
5. Написать метод, которому в качестве параметра передается целое число, метод должен напечатать в консоль положительное ли число передали, или отрицательное; Замечание: ноль считаем положительным числом.
6. Написать метод, которому в качестве параметра передается целое число, метод должен вернуть true, если число отрицательное;
7. Написать метод, которому в качестве параметра передается строка, обозначающая имя, метод должен вывести в консоль сообщение «Привет, указанное_имя!»;
8. * Написать метод, который определяет является ли год високосным, и выводит сообщение в консоль. Каждый 4-й год является високосным, кроме каждого 100-го, при этом каждый 400-й – високосный.*/
    public static void main (String[] args) {
        System.out.println("Hi Bro!");
        //2. Создать переменные и инициализация значений;
            /*byte b = 8;
            short s = 16;
            int i = 32;
            long l = 64L;
            float f = 4.6f;
            double d = (double) 0.88;

            char c = 'R';
            char ch = '\u12aa';
            System.out.println (ch);
            boolean bool = true;
            String str = "hi!";*/

        //3.вызов метода countExpression
        System.out.println (countExpression(5, 2, 8, 2));
        //4.вызов метода checkingMethod
        System.out.println (checkingMethod(5, 5));
        //5.вызов метода checkingPositiveNumber
        checkingPositiveNumber( 100);
        //6.вызов метода checkingPositiveNumberBool
        System.out.println ("Is it Positive Number? " + checkingPositiveNumberBool( -1));
        //7.вызов метода sayHi
        sayHi("Irun");
        //8.вызов метода leapYear
        leapYear(2020);

    }
    public static int countExpression (int a, int b, int c, int d){
        return a * (b + (c / d));

    }
    public static boolean checkingMethod (int a, int b) {
        int summ = a + b;
        if (summ >= 10 && summ <= 20) {
            return (2 == 2);
        } else{
            return (2 == 1);
        }
    }
    public static void checkingPositiveNumber (int a) {
        if (a >= 0) {
            System.out.println ("a is Positive Number");
        } else{
            System.out.println ("a is Negative Number");
        }
    }
    public static boolean checkingPositiveNumberBool (int a) {
        if (a >= 0) {
            return (2 == 2);
        } else{
            return (2 == 1);
        }
    }
    public static void  sayHi(String st) {
            System.out.println ("Hi " + st + " !");
    }
    //Каждый 4-й год является високосным, кроме каждого 100-го, при этом каждый 400-й – високосный
    public static void leapYear (int year){
         if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0))
             System.out.println (year + " is a leap year.");
         else
             System.out.println (year + " is not a leap year.");
    }
}