import java.util.Scanner;

public class TwoElementsSum {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        String[] sequence = input.split(" ");
        if (sequence.length < 3) {
            System.out.println(0);
            return;
        }

        int[] sequenceOfNumbers = new int[sequence.length];
        for (int i = 0; i < sequence.length; i++) {
            sequenceOfNumbers[i] = Integer.parseInt(sequence[i]);
        }

        int sum = sequenceOfNumbers[0] + sequenceOfNumbers[2];

        for (int i = 0; i < sequenceOfNumbers.length - 2; i++) {
            int currentSum = sequenceOfNumbers[i] + sequenceOfNumbers[i + 2];
            if (currentSum < sum) {
                sum = currentSum;
            }
        }
        System.out.println(sum);
    }
}