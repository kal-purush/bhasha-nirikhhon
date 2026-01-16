package task_7;

import java.util.Arrays;

/**
 * 1) Создать два числовых массива одинаковой длины и
 * ...попарно разделить элементы первого массива на
 * ...элементы второго массива.
 * 2) Если в знаменателе получается 0, вывести вместо
 * ...результата деления сообщение об ошибке. Возможная
 * ...ошибка при делении не должна повлиять на
 * ...последующее деление элементов.
 */

public class TwoArrays {
    public static void main(String[] args) {
        int[] array1 = new int[]{10, 9, 8, 18, 7, 6, 5};
        int[] array2 = new int[]{3, 2, 1, 0, 1, 2, 3};
        int[] arrayResult = new int[array1.length];

        for (int i = 0; i < array1.length; i++) {
            try {
                arrayResult[i] = array1[i] / array2[i];
                System.out.println(arrayResult[i]);
            } catch (ArithmeticException ex) {
                System.out.println("На 0 делить нельзя!");
            }

        }
    }
}