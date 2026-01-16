package task_10;

/**
 * Дано слово из четного числа букв. Поменять местами его
 * половины. Вывести полученную строку в верхнем регистре
 */

public class EvenNumberLetters {

    public static void main(String[] args) {

        String str = "Привет";

        int i = str.length();

        String str1 = str.substring(0, i / 2);
        String str2 = str.substring(i / 2, i);

        String finalString = str2.toUpperCase() + str1.toUpperCase();
        System.out.println(finalString);

    }

}