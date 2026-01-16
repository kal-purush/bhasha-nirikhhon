package ahmet_codes.week6;

public class RemoveDup {

    public static void main(String[] args) {

        String word = "AAABBBCCC";
        String resultWord = removeDup(word);

        System.out.println(resultWord);

    }

    private static String removeDup(String word) {
        String result = " ";
        for (int i = 0; i < word.length() - 1; i++) {

            if (result.charAt(result.length() - 1) != word.charAt(i)) {
                result += word.charAt(i);
                result = result.trim();
            }

        }
        return result;
    }


}

