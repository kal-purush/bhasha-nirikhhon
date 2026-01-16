import java.util.ArrayList;
import java.util.Scanner;

public class H1T1 {

    public static void main(String[] args) {
        System.out.print("Введите с консоли через пробел целые числа:");
        Scanner keyboard = new Scanner(System.in);
        ArrayList<Integer> arr = new ArrayList<>();
        String s = keyboard.nextLine();
        keyboard = new Scanner(s);
        while (keyboard.hasNextInt()) {
            arr.add(keyboard.nextInt());
        }
        System.out.println("Колличество введёных чисел:" + arr.size());

        int count = 0;
        for (int i = 0; i < arr.size(); i++) {
            if (arr.get(i) > 0) {
                count++;
            }
        }
        System.out.println("Колличество положительных чисел:" + count);
    }
}