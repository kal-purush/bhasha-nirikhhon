import java.util.Scanner;

public class stringFunction {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String word;

        System.out.print("Enter a word: ");
        word = scanner.nextLine();

        for (int i = 0;i < word.length();i++) {
            System.out.println(word.charAt(i));
        }
    }
}