package GeekBrians.Lesson_1.Homework;

public class Main {

    public static void main(String[] args) {
//        task_1.main(args);
//        task_2.main(args);
//        task_3.main(args);
//        task_4.main(args);
//        task_5.main(args);
//        task_6.main(args);
//        task_7.main(args);
        task_8.main(args);
    }
}

// 1. Создать пустой проект в IntelliJ IDEA и прописать метод main().
class task_1{
    public static void main(String[] args) {

    }
}

// 2. Создать переменные всех пройденных типов данных и инициализировать их значения.
class task_2{
    public static void main(String[] args) {
        byte v1 = 8;
        short v2 = 16;
        int v3 = 32;
        long v4 = 64;
        float v5 = 32.0f;
        double v6 = 64.0;
        char v7 = 16;
        boolean v8 = false;
        String v9 = "Hello World!";
    }
}

// 3. Написать метод вычисляющий выражение a * (b + (c / d)) и возвращающий результат,
//    где a, b, c, d – аргументы этого метода, имеющие тип float.
class task_3{
    public static void main(String[] args) {
        float res = compute(3.1416f, 2.7183f, 1.4142f, 1.7321f);
        System.out.println(res);
    }
    private static float compute(float a, float b, float c, float d){
        return  a * (b + (c / d));
    }
}

// 4.Написать метод, принимающий на вход два целых числа и проверяющий,
//   что их сумма лежит в пределах от 10 до 20 (включительно),
//   если да – вернуть true, в противном случае – false.
class task_4{
    public static void main(String[] args) {
        System.out.println( isSumInTheRange(20, -5) );
    }
    private static boolean isSumInTheRange(int a, int b){
        return (10 < a+b) && (a+b <= 20);
    }
}

// 5. Написать метод, которому в качестве параметра передается целое число,
//    метод должен напечатать в консоль, положительное ли число передали или отрицательное.
//    Замечание: ноль считаем положительным числом.
class task_5{
    public static void main(String[] args) {
        printNumSign(42);
        printNumSign(0);
        printNumSign(-42);
    }
    private static void printNumSign(int num){
        System.out.print("Число " + num + " - ");
        if(num < 0)
            System.out.println("отрицательное");
        else
            System.out.println("положительное");
    }
}

// 6. Написать метод, которому в качестве параметра передается целое число.
//    Метод должен вернуть true, если число отрицательное, и вернуть false если положительное.
class task_6{
    public static void main(String[] args) {
        int num1 = 42, num2 = -42;
        System.out.println("Число " + num1 + " положительное - " + isPos(num1));
        System.out.println("Число " + num2 + " положительное - " + isPos(num2));
    }
    private static boolean isPos(int num){
        return !(num < 0);
    }
}
// 7. Написать метод, которому в качестве параметра передается строка, обозначающая имя.
// Метод должен вывести в консоль сообщение «Привет, указанное_имя!».
class task_7{
    public static void main(String[] args) {
        sayHiTo("Мир");
    }
    private static void sayHiTo(String name){
        System.out.println("Привет, " + name + "!");
    }
}

// 8. *Написать метод, который определяет, является ли год високосным,
//     и выводит сообщение в консоль. Каждый 4-й год является високосным,
//     кроме каждого 100-го, при этом каждый 400-й – високосный.
class task_8{
    public static void main(String[] args) {
        printIsLeapYear(2020);
        printIsLeapYear(2021);
        printIsLeapYear(2000);
        printIsLeapYear(1764);
        printIsLeapYear(1100);
    }
    private static void printIsLeapYear(int year){
        System.out.print("Год " + year + " - ");
        if((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
            System.out.println("високосный");
        else
            System.out.println("не високосный");
    }
}