package preX.pre9;

/**
 * Created by TJ Challstrom on 21-Jun-15.
 * For array sorting
 */
public class basicSorting {
    static int[] ints = {10,3,5,9};

    public static void arrayPrint(int[] numbers) {
        for (int i = 0; i < numbers.length; i++) {
            System.out.println(numbers[i]);
        }
    }

    public static boolean arrayValidate(int[] source, int[] compare) {
        int check = source.length;
        for (int i = 0; i < source.length; i++) {
            for (int j = 0; j < compare.length; j++) {
                if(source[i] == compare[j]) {
                    check--;
                }
            }
        }
        return check == 0;
    }

    public static void insertionSort(int[] numbers) {
        for (int i = 0; i < numbers.length; i++) { //Insertion Sort, kinda
            for (int j = i; j < numbers.length; j++) {
                if (numbers[j] < numbers[i]){
                    int temp = numbers[i];
                    numbers[i] = numbers[j];
                    numbers[j] = temp;
                }
            }
        }
    }
}