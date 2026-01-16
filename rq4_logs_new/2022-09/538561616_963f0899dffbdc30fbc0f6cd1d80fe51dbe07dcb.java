import java.util.Arrays;

public class zd10 {
    public static void main(String[] arg) {
        tenthTask();
    }
    public static void tenthTask() {
        int[] numbers = {15,10,51,6,5,3,10,-34,0,32,56,0,24,52};
        int counterZeros = 0;
        for (int i = 0; i < 14; i++) {
            if (numbers[i] == 0) {
                counterZeros++;
                deletElement(numbers, i);
            }
        }
        if (counterZeros > 0) {
            for (int i = 0; i < counterZeros; i++) {
                numbers = Arrays.copyOf(numbers, numbers.length + 1);
                numbers[numbers.length - 1] = 0;
            }
        }
        System.out.println(Arrays.toString(numbers));
    }
    public static void deletElement(int [] array, int index) {
        for (int j = index + 1; j < array.length; j++) {
            array[j - 1] = array[j];
        }
        array = Arrays.copyOf(array, array.length - 1);
    }
}