import java.util.Arrays;

public class ExtractPositiveIntNumbersFromString {
    public static void main(String[] args) {
        String string = "Hello 1234 from 23, or java - is not java11!-2.3 is not a 24";
        int[] numbers = extractNumbersFromString(string);
        System.out.println(Arrays.toString(numbers));
    }

        private static int[] extractNumbersFromString (String string){
            StringBuilder stringBuilder = new StringBuilder();
            int count = 0;
            for (int i = 0; i < string.length(); i++) {
                if (Character.isDigit(string.charAt(i))) {
                    stringBuilder.append(string.charAt(i));
                    count++;
                } else if (!Character.isDigit(string.charAt(i)) && count > 0) {
                    stringBuilder.append(" ");
                    count = 0;
                }
            }

            String[] strArray = stringBuilder.toString().split(" ");
            int[] newArray = new int[strArray.length];

            for (int i = 0; i < strArray.length; i++) {
                newArray[i] = Integer.parseInt(strArray[i]);
            }

            return newArray;
        }
    }

