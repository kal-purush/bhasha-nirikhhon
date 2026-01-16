public class WordMatch {

    //properties
    private String secretString;
    //constructors

    public WordMatch(String secretString){
        this.secretString = secretString;
    }

    //methods
    public int scoreGuess(String guess){
        //throws IllegalArgumentException empty String
        if (guess.length() < 1){
            throw new IllegalArgumentException("Your guess cannot be an empty String!");
        }
        //throws IllegalArgumentException exceed length of secret word
        if (guess.length() > this.secretString.length()){
            throw new IllegalArgumentException("Your guess cannot exceed the number of characters in the Secret word");
        }

        //occurrence counts how many times guess appears in secretString
        int occurrence = 0;
        /**e.g. guess "iss" for "mississippi", guess length = 3, secretString length = 11
         * "ppi" is the last substring to be checked, or else OutOfBounds
         * so index must end at index 8
         * index < 11 - 3 + 1, in this case index < 9
         */
        for (int i = 0; i < secretString.length() - guess.length() + 1; i++){
            //substring goes from i, i+1, i+2, ... i + guess.length - 1
            String substring = secretString.substring(i, i + guess.length());
            if (guess.equals(substring)){
                occurrence++;
            }
        }
        return occurrence * guess.length() * guess.length();
    }

    public String findBetterGuess(String guess1, String guess2){
        if (scoreGuess(guess1) > scoreGuess(guess2)){
            return guess1;
        }
        else if (scoreGuess(guess1) < scoreGuess(guess2)){
            return guess2;
        }
        //else if scoreGuess same, alphabetically greater guess is returned
        else{
            //using Math.min(guess1.length(), guess2.length()) to prevent OutOfBounds Error
            for (int i = 0; i < Math.min(guess1.length(), guess2.length()); i++){
                /**if char at index i are equal, program continues checking next letter
                 * if char at index i are not equal, then the one with greater ASCII value is returned
                 * e.g. in cat and con, 1st char 'c' is the same
                 * program moves to the next char, char 'o' >  char 'a'
                 * Therefore con has larger alphabet value, con is returned
                 */
                 if ((int)guess1.charAt(i) > (int)guess2.charAt(i)){
                     return guess1;
                 }
                 else if ((int)guess1.charAt(i) < (int)guess2.charAt(i)){
                     return guess2;
                 }
            }

            //if scoreGuess and alphabet order are EXACT SAME, return any one of them is fine
            return guess1;
        }
    }
}