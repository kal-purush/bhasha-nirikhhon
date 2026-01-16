import java.util.Scanner;

public class Task1 {
    public static void main(String[] args) {
        int count = 0;
        System.out.println("Введите числа через пробел");
        Scanner console = new Scanner(System.in);
        if (console.hasNextInt()) {
            String[] str = console.nextLine().split(" ");
            for (String s : str) {
                int i = Integer.parseInt(s);
                if (i > 0) {
                    count++;
                }
            }
            System.out.println(count);
        }
        else {
            System.out.println("Вы не вввели числа через пробел");
        }
    }
}