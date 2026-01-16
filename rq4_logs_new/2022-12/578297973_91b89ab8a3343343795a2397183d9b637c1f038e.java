import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.*;

/*  Задача 2. Реализуйте алгоритм сортировки пузырьком числового массива, результат после каждой
 итерации запишите в лог-файл. 
*/

public class SemTwoTaskTwo {
    public static final Logger logger = Logger.getLogger(SemTwoTaskTwo.class.getName());

    public static void main(String[] args) throws IOException {
        FileHandler fh = new FileHandler("log2.txt");
        logger.addHandler(fh);
        SimpleFormatter sFormat = new SimpleFormatter();
        fh.setFormatter(sFormat);
        List<Integer> arr = creatingArray();
        bubbleSorting((ArrayList<Integer>) arr);
    }

    public static List<Integer> creatingArray() throws IOException {
        int a;
        ArrayList <Integer> arr = new ArrayList<>();
        System.out.println("Введите количество чисел в массиве: ");
        Scanner in = new Scanner(System.in);
        a = in.nextInt();
            for (int i = 0; i <= a-1; i++) {
            arr.add ((int) (Math.random()*100));
        }
        in.close();
        return arr;
    }

    public static void bubbleSorting(ArrayList <Integer> arr) {
        for (int i = 0; i < arr.size() - 1; i++) {
            for (int j = 0; j < arr.size() - 1; j++) {
                if (arr.get(j + 1) < arr.get(j)) {
                    int swap = arr.get(j);
                    arr.set(j, arr.get(j + 1));
                    arr.set(j + 1, swap);
                    logger.log (Level.INFO, String.valueOf(arr));
                }
            }
        }
    }
}