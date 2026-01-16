import java.util.Scanner;

public class Cipher {
    public static StringBuffer encrypt(String text, int s, boolean encrypt) {
        StringBuffer result = new StringBuffer();

        for (int i = 0; i < text.length(); i++) {
            char originalChar = text.charAt(i);

            if (Character.isAlphabetic(originalChar)) {
                int offset = Character.isUpperCase(originalChar) ? 'A' : 'a';
                int shiftedChar = encrypt ?
                        ((originalChar - offset + s) % 26 + offset) :
                        ((originalChar - offset - s + 26) % 26 + offset);

                result.append((char) shiftedChar);
            } else {
                result.append(originalChar);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the text: ");
        String text = scanner.nextLine();

        System.out.print("Enter the shift value (integer): ");
        int s = scanner.nextInt();

        System.out.println("Choose an option: Press 1. for encoding 2. for decoding");

        int option = scanner.nextInt();

        if (option == 1) {
            System.out.println("Text : " + text);
            // System.out.println("Shift : " + s);
            System.out.println("Encoded text: " + encrypt(text, s, true));
        } else if (option == 2) {
            System.out.println("Text : " + text);
            // System.out.println("Shift : " + s);
            System.out.println("Decoded Text: " + encrypt(text, s, false));
        } else {
            System.out.println("Error 404");
        }

        scanner.close();
    }
}