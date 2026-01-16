package GeekBrians.Slava_5655380.Homework.Lesson11;

import java.util.ArrayList;

public class ArrayUtil {
    /**
     * ЗАДАНИЕ 1. Написать метод, который меняет два элемента массива местами (массив может быть любого ссылочного типа);
     */
    public static <E> void swapElements(E[] arr, int firstIndex, int secondIndex) {
        E temp = arr[firstIndex];
        arr[firstIndex] = arr[secondIndex];
        arr[secondIndex] = temp;
    }

    /**
     * ЗАДАНИЕ 2. Написать метод, который преобразует массив в ArrayList;
     */
    public static <E> ArrayList<E> toArrayList(E[] srcArr) {
        ArrayList<E> arrayList = new ArrayList<>(srcArr.length);
        for (int i = 0; i < srcArr.length; i++)
            arrayList.add(i, srcArr[i]);
        return arrayList;
    }
}