package lesson4.zd2;

public class zd2 {
    /**
     Задача 2:
     2.1 Создать массив fruits и заполнить его 4 произвольными фруктами.
     2.2 вывести в консоль второй и четвертый.
     2.3 вывести в консоль длину массива.
     2.4 третий фрукт заменить на иной.
     2.5 проверить результат в консоли
     */
    public static void main(String[] args) {
        String [] fruts = new String[]{"apple", "banana","orange","apricot"};
        System.out.println("второй " + fruts[1] + " четвертый " + fruts[3]);
        System.out.println("длина массива " + fruts.length);
        fruts[2] = "banana";
        System.out.println("третий фрукт " + fruts[2]);
    }
}