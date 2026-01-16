package ahmet_codes.week7;

public class UniqueCharacters {

    public static void main(String[] args) {
        String word = "AAABBBCCCDEF"; // given String

        String uniqueChar = uniqueCharsMethod(word);
        // The string for unique characters

        System.out.println("The Unique Characters: "+uniqueChar);
    }

    public static String uniqueCharsMethod(String word) {

        // Create a empty string for unique characters
        String uniqueChar = "";

        ////////Check each character of given String. this string come from main.
        for (int i = 0; i < word.length(); i++) {
            char currentChar = word.charAt(i);     // to get Current Character


            //////// If this character is here only once
            int count = 0;       // there is a number for repetition

            ///// This for loop counts how many times this character has used
            for (int j = 0; j < word.length(); j++) {
                if (word.charAt(j) == currentChar) {  //if both characters is equal
                    count++;                          //increase the count number
                }
            }

            ///////// If the character only marked once time, add current character to unique character String.
            //if (count == 1 && !uniqueChar.contains(String.valueOf(currentChar))) {
            if (count == 1 ) {
                uniqueChar += String.valueOf(currentChar);// to change the type of currentChar to String
            }
        }


        return uniqueChar;
    }
}