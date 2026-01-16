package GeekBrians.Slava_5655380.Homework;

import static GeekBrians.Slava_5655380.Util.*;

public class Lesson_2 {

    public static void main(String[] args) {
        //FirstTask.main(args);
        //SecondTask.main(args);
        //ThirdTask.main(args);
        //FourthTask.main(args);
        //FifthTask.main(args);
        //SixthTask.main(args);
        SeventhTask.main(args);
    }
}

// 1. Задать целочисленный массив, состоящий из элементов 0 и 1. Например: [ 1, 1, 0, 0, 1, 0, 1, 1, 0, 0 ]. С помощью цикла и условия заменить 0 на 1, 1 на 0;
class FirstTask {
    public static void main(String[] args) {
        int[] intArr = {1, 1, 0, 0, 1, 0, 1, 1, 0, 0};
        for (int i = 0; i < intArr.length; i++)
            intArr[i] = (intArr[i] == 0) ? 1 : 0;
        printArr(intArr);
    }
}

// 2. Задать пустой целочисленный массив размером 8. С помощью цикла заполнить его значениями 0 3 6 9 12 15 18 21;
class SecondTask {
    public static void main(String[] args) {
        int[] intArr = new int[8];
        for (int i = 0; i < intArr.length; i++)
            intArr[i] = i * 3;
        printArr(intArr);
    }
}

// 3. Задать массив [ 1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1 ] пройти по нему циклом, и числа меньшие 6 умножить на 2;
class ThirdTask {
    public static void main(String[] args) {
        int[] arr = {1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1};
        for (int i = 0; i < arr.length; i++)
            if (arr[i] < 6)
                arr[i] = arr[i] * 2;
        printArr(arr);
    }
}


// 4. Создать квадратный двумерный целочисленный массив (количество строк и столбцов одинаковое), и с помощью цикла(-ов) заполнить его диагональные элементы единицами;
class FourthTask {
    public static void main(String[] args) {
        int[][] squareMatrix = new int[5][5];
        for (int i = 0, j = 0; i < squareMatrix.length && j < squareMatrix[i].length; i++, j++)
            squareMatrix[i][j] = 1;
        for (int i = 0, j = squareMatrix[i].length - 1; i < squareMatrix.length && j >= 0; i++, j--)
            squareMatrix[i][j] = 1;
        for (int i = 0; i < squareMatrix.length; i++) {
            printArr(squareMatrix[i]);
            System.out.println();
        }

    }
}

// 5. ** Задать одномерный массив и найти в нем минимальный и максимальный элементы (без помощи интернета);
class FifthTask {
    public static void main(String[] args) {
        int[] arr = generateAndPrintRandomArr();
        printArr(arr);
        System.out.println("\nmin: " + getArrMin(arr) + ", max: " + getArrMax(arr));
    }

    private static int getArrMin(int[] arr) {
        int min = arr[0];
        for (int i = 1; i < arr.length; i++)
            if (arr[i] < min)
                min = arr[i];
        return min;
    }

    private static int getArrMax(int[] arr) {
        int max = arr[0];
        for (int i = 1; i < arr.length; i++)
            if (arr[i] > max)
                max = arr[i];
        return max;
    }
}

// 6. ** Написать метод, в который передается не пустой одномерный целочисленный массив, метод должен вернуть true,
// если в массиве есть место, в котором сумма левой и правой части массива равны. Примеры: checkBalance([2, 2, 2, 1, 2, 2, || 10, 1]) → true,
// checkBalance([1, 1, 1, || 2, 1]) → true, граница показана символами ||, эти символы в массив не входят.
class SixthTask {
    public static void main(String[] args) {
        int[] arr = generateAndPrintRandomArr();
        //int[] arr = {2, 2, 2, 1, 2, 2, 10, 1};
        //int[] arr = {1, 1, 1, 2, 1};
        System.out.print("arr: ");
        printArr(arr);
        System.out.println("\nisArrBalanced: " + isArrBalanced(arr));
    }

    private static int getRangeSum(int[] arr, int begin, int end) {
        if (begin > end) {
            int tmp = begin;
            begin = end;
            end = tmp;
        }
        int sum = 0;
        for (int i = begin; i < end; i++)
            sum += arr[i];
        return sum;
    }

    private static boolean isArrBalanced(int[] arr) {
        for (int i = 0; i < arr.length; i++)
            if (getRangeSum(arr, 0, i) == getRangeSum(arr, i, arr.length))
                return true;
        return false;
    }
}

// 7. **** Написать метод, которому на вход подается одномерный массив и число n (может быть положительным, или отрицательным),
// при этом метод должен сместить все элементы массива на n позиций. Элементы с мещаются циклично.
// Для усложнения задачи нельзя пользоваться вспомогательными массивами.
// Примеры: [ 1, 2, 3 ] при n = 1 (на один вправо) -> [ 3, 1, 2 ];
// [ 3, 5, 6, 1] при n = -2 (на два влево) -> [ 6, 1, 3, 5 ]. При каком n в какую сторону сдвиг можете выбирать сами.
class SeventhTask {
    public static void main(String[] args) {
        int[] arr = generateAndPrintRandomArr();
        int n = new java.util.Random().nextInt(arr.length * 2);
        shiftArr(arr, n);
        System.out.print("\narr shifted by " + n + ": ");
        for (var i : arr)
            System.out.print(i + " ");
    }

    private static void shiftArr(int[] arr, int n) {
        n %= arr.length;
        for (int i = 0; i < n; i++)
            shiftArr(arr);
    }

    private static void shiftArr(int[] arr) {
        int lastEl = arr[arr.length - 1];
        for (int i = arr.length - 1; i > 0; i--)
            arr[i] = arr[i - 1];
        arr[0] = lastEl;
    }
}