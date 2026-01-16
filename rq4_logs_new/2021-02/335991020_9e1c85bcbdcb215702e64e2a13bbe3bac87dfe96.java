package GeekBrians.Slava_5655380.Homework.Lesson14.Task1;

public class Class {
    public static int[] method(int[] arr) {
        int[] resArr = null;
        for (int i = arr.length - 1; i >= 0; i--)
            if(arr[i] == 4){
                resArr = new int[arr.length - 1 - i];
                System.arraycopy(arr, i + 1, resArr, 0, resArr.length);
                return resArr;
            }
        throw new RuntimeException("В исходном массиве нет значения равного четырём");
    }
}