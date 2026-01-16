import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        Queue<Integer> list = new LinkedList<>();
        Scanner scan = new Scanner(System.in);

        int i = 0;
        int sum = 0;
        Integer number = 0;
        while (i < 5){

            number = scan.nextInt();
            i++;
            ((LinkedList<Integer>) list).add(number);

            sum = sum + number;
        }


        for (Integer lista: list) {
            System.out.printf("%s + ", ((LinkedList<Integer>) list).get(lista - 1));
        }

        System.out.println(sum);

    }
}