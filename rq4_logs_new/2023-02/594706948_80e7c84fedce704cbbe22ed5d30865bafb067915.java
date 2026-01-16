package src.main.java.homework7;

public class Main {

    public static void main(String[] args) {

        findSymbolOccurance("Test my character string", 't');
        findWordPosition("Съешь еще этих мягких булок.", "булок");
        stringReverse("Бубочка");
        isPalindrome("шалаш");

    }

    static int findSymbolOccurance(String string, char character) {

        int characterCount = 0;
        for (int i = 0; i < string.length(); i++) {

            if (string.charAt(i) == character) {
                characterCount++;
            }

        }
        System.out.println(characterCount);  //just for test notification
        return characterCount;

    }

    static int findWordPosition(String source, String target) {
        return source.indexOf(target);
    }

    static String stringReverse(String string) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = string.length() - 1; i >= 0; i--) {
            stringBuilder.append(string.charAt(i));
        }
        System.out.println(stringBuilder.toString());
        return stringBuilder.toString();
    }

    static boolean isPalindrome(String input) {

        return stringReverse(input).equals(input);

    }


//    static String guessWord(String input){
//
//        String[] words = {"apple", "orange", "lemon", "banana", "apricot", "avocado" , "broccoli", "carrot", "cherry", "garlic", "grape",
//                "melon", "leak", "kiwi", "mango", "mushroom", "nut", "olive", " pea", "peanut", "pear", "pepper", "pineapple", "pumpkin", "potato"};
//
//
//
//    }

}