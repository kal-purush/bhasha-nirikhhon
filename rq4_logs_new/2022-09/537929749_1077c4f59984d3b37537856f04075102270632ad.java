import components.set.Set;
import components.set.Set1L;
import components.simplereader.SimpleReader;
import components.simplereader.SimpleReader1L;
import components.simplewriter.SimpleWriter;
import components.simplewriter.SimpleWriter1L;

/**
 * Put a short phrase describing the program here.
 *
 * @author Jessie Kong
 *
 */
public final class WordleSolver {

    /**
     * Private constructor so this utility class cannot be instantiated.
     */
    private WordleSolver() {
    }

    /**
     * Generates the set of characters in the given {@code String} into the
     * given {@code Set}.
     *
     * @param sepStr
     *            the given {@code String}
     * @param sepSet
     *            the {@code Set} to be replaced
     * @replaces charSet
     * @ensures charSet = entries(sepStr)
     */
    private static void generateElements(String sepStr, Set<Character> sepSet) {
        assert sepStr != null : "Violation of: str is not null";
        assert sepSet != null : "Violation of: charSet is not null";

        // Clear the character set
        sepSet.clear();

        // Add every (unique) character in the string to the set
        for (int i = 0; i < sepStr.length(); i++) {
            // Only add the character if it isn't already in the set
            if (!sepSet.contains(sepStr.charAt(i))) {
                sepSet.add(sepStr.charAt(i));
            }
        }

    }

    /**
     * Returns the first "word" (maximal length string of characters not in
     * {@code separators}) or "separator string" (maximal length string of
     * characters in {@code separators}) in the given {@code text} starting at
     * the given {@code position}.
     *
     * @param text
     *            the {@code String} from which to get the word or separator
     *            string
     * @param position
     *            the starting index
     * @param sepSet
     *            the {@code Set} of separator characters
     * @return the first word or separator string found in {@code text} starting
     *         at index {@code position}
     * @requires 0 <= position < |text|
     * @ensures <pre>
     * nextWordOrSeparator =
     *   text[position, position + |nextWordOrSeparator|)  and
     * if entries(text[position, position + 1)) intersection separators = {}
     * then
     *   entries(nextWordOrSeparator) intersection separators = {}  and
     *   (position + |nextWordOrSeparator| = |text|  or
     *    entries(text[position, position + |nextWordOrSeparator| + 1))
     *      intersection separators /= {})
     * else
     *   entries(nextWordOrSeparator) is subset of separators  and
     *   (position + |nextWordOrSeparator| = |text|  or
     *    entries(text[position, position + |nextWordOrSeparator| + 1))
     *      is not subset of separators)
     * </pre>
     */
    private static String nextWordOrSeparator(String text, int position,
            Set<Character> sepSet) {
        assert text != null : "Violation of: text is not null";
        assert 0 <= position : "Violation of: 0 <= position";
        assert position < text.length() : "Violation of: position < |text|";

        // Stores whether the first character is a separator or not
        boolean isSep = sepSet.contains(text.charAt(position));

        // Begin iterating after the first character
        int i = position + 1;
        while ((i < text.length())
                && (isSep == sepSet.contains(text.charAt(i)))) {
            // As long as the current character matches the first character's state
            // (separator or not a separator), continue looking through characters
            i++;
        }

        return text.substring(position, i);
    }

    /**
     * Creates string of status codes given candidate and true solution.
     *
     * @param candidate
     *            possible word
     * @param solution
     *            correct word
     *
     * @return string of status codes
     */
    private static String generateStatus(String candidate, String solution) {
        String status = "";

        for (int i = 0; i < candidate.length(); i++) {
            if (candidate.charAt(i) == solution.charAt(i)) {
                status = status + 'g';
            } else if (solution.indexOf(candidate.charAt(i)) < 0) {
                status = status + 'r';
            } else {
                status = status + 'y';
            }
        }
        return status;
    }

    /**
     * Prints number of remaining possible words.
     *
     * @param out
     *            stream to print
     * @param wordsLeft
     *            the {@code Set} of remaining possible words
     *
     */
    private static void printRemainingPossibilities(SimpleWriter out,
            Set<String> wordsLeft) {
        assert out != null : "Violation of: text is not null";
        assert wordsLeft != null : "Violation of: wordsLeft is not null";

        // Number of possibilities to print
        final int maxPoss = 10;
        int numPoss = Integer.min(maxPoss, wordsLeft.size());

        if (wordsLeft.size() > 1) {
            out.println("Remaining possiblities: " + wordsLeft.size() + "\n");
            out.println(
                    numPoss + " possibilities (of " + wordsLeft.size() + "): ");

            // Print possibilities, storing removed words in temp
            Set<String> temp = wordsLeft.newInstance();
            while (temp.size() < numPoss) {
                String removed = wordsLeft.removeAny();
                temp.add(removed);
                out.println("\t" + removed);
            }
            // Transfer words from temp back to wordsLeft
            while (temp.size() > 0) {
                String removed = temp.removeAny();
                wordsLeft.add(removed);
            }
            out.println("\n--------------------------------------------\n");
        } else if (wordsLeft.size() == 1) {
            out.println("ONE POSSIBILITY REMAINING!!");
            out.println(
                    "\n\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$'               `$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  \r\n"
                            + "$$$$$$$$$$$$$$$$$$$$$$$$$$$$'                   `$$$$$$$$$$$$$$$$$$$$$$$$$$$$\r\n"
                            + "$$$'`$$$$$$$$$$$$$'`$$$$$$!                       !$$$$$$'`$$$$$$$$$$$$$'`$$$\r\n"
                            + "$$$$  $$$$$$$$$$$  $$$$$$$                         $$$$$$$  $$$$$$$$$$$  $$$$\r\n"
                            + "$$$$. `$' \\' \\$`  $$$$$$$!                         !$$$$$$$  '$/ `/ `$' .$$$$\r\n"
                            + "$$$$$. !\\  i  i .$$$$$$$$                           $$$$$$$$. i  i  /! .$$$$$\r\n"
                            + "$$$$$$   `--`--.$$$$$$$$$                           $$$$$$$$$.--'--'   $$$$$$\r\n"
                            + "$$$$$$L        `$$$$$^^$$                           $$^^$$$$$'        J$$$$$$\r\n"
                            + "$$$$$$$.   .'   \"\"~   $$$    $.                 .$  $$$   ~\"\"   `.   .$$$$$$$\r\n"
                            + "$$$$$$$$.  ;      .e$$$$$!    $$.             .$$  !$$$$$e,      ;  .$$$$$$$$\r\n"
                            + "$$$$$$$$$   `.$$$$$$$$$$$$     $$$.         .$$$   $$$$$$$$$$$$.'   $$$$$$$$$\r\n"
                            + "$$$$$$$$    .$$$$$$$$$$$$$!     $$`$$$$$$$$'$$    !$$$$$$$$$$$$$.    $$$$$$$$\r\n"
                            + "$$$$$$$     $$$$$$$$$$$$$$$$.    $    $$    $   .$$$$$$$$$$$$$$$$     $$$$$$$\r\n"
                            + "                                 $    $$    $\r\n"
                            + "                                 $.   $$   .$\r\n"
                            + "                                 `$        $'\r\n"
                            + "                                  `$$$$$$$$'");
            out.println("\nSOLUTION: " + wordsLeft.removeAny());
        } else {
            out.println("Remaining possiblities: 0");
            out.println("Hmm... are you sure you're playing today's Wordle?");
        }
    }

    /**
     * Eliminates words given a character's index and status.
     *
     * @param index
     *            position of character in word
     * @param letter
     *            the character
     * @param status
     *            relationship between character and word (either r, y, or g)
     * @param wordsLeft
     *            the {@code Set} of remaining possible words
     * @return number of words eliminated
     */
    private static int eliminateWordsFromChar(int index, char letter,
            char status, Set<String> wordsLeft) {
        assert wordsLeft != null : "Violation of: wordsLeft is not null";

        int eliminated = wordsLeft.size();

        // Transfer all words to wordBank
        Set<String> wordBank = wordsLeft.newInstance();
        wordBank.transferFrom(wordsLeft);

        switch (status) {
            case 'r': {
                // Remove all words with the letter
                while (wordBank.size() > 0) {
                    String candidate = wordBank.removeAny();
                    // If candidate doesn't contain the letter, add it back
                    if (candidate.indexOf(letter) < 0) {
                        wordsLeft.add(candidate);
                    }
                }
                break;
            }
            case 'y': {
                // Remove all words without the letter, or with letter at that position
                while (wordBank.size() > 0) {
                    String candidate = wordBank.removeAny();
                    // If candidate contains the letter, add it back
                    if (candidate.indexOf(letter) >= 0
                            && candidate.indexOf(letter) != index) {
                        wordsLeft.add(candidate);
                    }
                }
                break;
            }
            case 'g': {
                // Remove all words without the letter in specific position
                while (wordBank.size() > 0) {
                    String candidate = wordBank.removeAny();
                    // If candidate has letter at specific position, add it back
                    if (candidate.charAt(index) == letter) {
                        wordsLeft.add(candidate);
                    }
                }
                break;
            }
            default: {
                break;
            }
        }

        eliminated -= wordsLeft.size();
        return eliminated;
    }

    /**
     * Eliminates words given a word and its status string.
     *
     * @param word
     *            the guess
     * @param status
     *            string of codes (characters: r, y, or g)
     * @param wordsLeft
     *            the {@code Set} of remaining possible words
     * @return number of words eliminated
     */
    private static int eliminateWords(String word, String status,
            Set<String> wordsLeft) {
        int eliminated = 0;
        for (int i = 0; i < word.length(); i++) {
            eliminated += eliminateWordsFromChar(i, word.charAt(i),
                    status.charAt(i), wordsLeft);
        }
        return eliminated;
    }

    /**
     * Calculates the number of words the candidate elimianted on average.
     *
     * @param word
     *            candidate
     * @param wordsLeft
     *            the {@code Set} of remaining possible words
     * @return average number of words eliminated
     */
    private static int avgWordsEliminated(String word, Set<String> wordsLeft) {
        assert wordsLeft != null : "Violation of: wordsLeft is not null";

        int sumEliminated = 0;

        // Create copy of wordsLeft, and array
        Set<String> origWordsLeft = wordsLeft.newInstance();
        String[] wordsLeftArray = new String[wordsLeft.size()];
        int wordNum = 0;
        for (String str : wordsLeft) {
            origWordsLeft.add(str);
            wordsLeftArray[wordNum] = str;
            wordNum++;
        }

        // Iterates through every possible solution
        for (int i = 0; i < wordsLeftArray.length; i++) {
            String possibleSolution = wordsLeftArray[i];
            String status = generateStatus(word, possibleSolution);
            // Sum of words eliminated for each possible solution
            sumEliminated += eliminateWords(word, status, wordsLeft);
            // Restore wordsLeft
            for (String str : origWordsLeft) {
                if (!wordsLeft.contains(str)) {
                    wordsLeft.add(str);
                }
            }
        }

        return (int) Math.round((sumEliminated * 1.0) / wordsLeft.size());
    }

    /**
     * Determines word that eliminates most words on average.
     *
     * @param wordsLeft
     *            the {@code Set} of remaining possible words
     * @param out
     *            output stream
     * @return average number of words eliminated
     */
    private static String printBestNextGuess(Set<String> wordsLeft,
            SimpleWriter out) {
        assert wordsLeft != null : "Violation of: wordsLeft is not null";

        // Create copy of wordsLeft, and array
        Set<String> origWordsLeft = wordsLeft.newInstance();
        String[] wordsLeftArray = new String[wordsLeft.size()];
        int wordNum = 0;
        for (String str : wordsLeft) {
            origWordsLeft.add(str);
            wordsLeftArray[wordNum] = str;
            wordNum++;
        }

        String bestGuess = "";
        int maxAvg = -1;
        int avg;

        for (int i = 0; i < wordsLeftArray.length; i++) {
            String possibleGuess = wordsLeftArray[i];
            avg = avgWordsEliminated(possibleGuess, wordsLeft);
            //out.println(avg);
            if (avg > maxAvg) {
                maxAvg = avg;
                bestGuess = possibleGuess;
            }
        }

//        Queue<Map.Pair<String, Integer>> guesses = new Queue1L<Map.Pair<String, Integer>>();

//        // Iterates through every possible next guess (wordsLeft)
//        for (int i = 0; i < wordsLeft.size(); i++) {
//            String possibleGuess = wordsLeft.removeAny();
//            sumEliminated += eliminateWords(word, status, wordsLeft);
//        }
//
        out.println("BEST NEXT GUESS: " + bestGuess);
        return bestGuess;
    }

    /**
     * Main method.
     *
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) {
        SimpleReader in = new SimpleReader1L();
        SimpleWriter out = new SimpleWriter1L();

        // Can increase, in case user would like eliminate past words
        int wordleDay = 0;
        // Keep track of number of guesses
        int guessNum = 1;

        // DICTIONARY
        SimpleReader dicWords = new SimpleReader1L("data/dictionaryWords.txt");
        // Open allWords file
        SimpleReader allWords = new SimpleReader1L("data/allWords.txt");
        String line = allWords.nextLine();
        // Create separator set
        String sepStr = " ,\"\'\n";
        Set<Character> sepSet = new Set1L<Character>();
        generateElements(sepStr, sepSet);

        // Old words - populate with past Wordle words
        Set<String> oldWords = new Set1L<String>();
        // New words
        Set<String> wordsLeft = new Set1L<String>();
        // All five-letter words - include words outside of the Wordle word bank
        Set<String> allFiveLetterWords = new Set1L<String>();

        // Put this in function getPossibleWords();
        int pos = 0;
        while (pos < line.length()) {
            // Token will either be a word or separator
            String token = nextWordOrSeparator(line, pos, sepSet);
            // If the token is a word
            if (!sepSet.contains(token.charAt(0))) {
                // Add word to either oldWords or newWords
                if (oldWords.size() < wordleDay) {
                    oldWords.add(token);
                } else {
                    wordsLeft.add(token);
                }
                allFiveLetterWords.add(token);
            }
            pos += token.length();
        }

        while (!dicWords.atEOS()) {
            String token = dicWords.nextLine();
            if (!allFiveLetterWords.contains(token)) {
                allFiveLetterWords.add(token);
            }
        }

        // Greet user
        out.println(
                "Hello! Welcome to the Wordle Solver. Enter your first guess below...\n");
        out.println("BEST NEXT GUESS: raise");

        while (wordsLeft.size() > 1) {
            int eliminated = 0;
            // Ask for word
            out.print("Enter word (guess #" + guessNum + "): ");
            String word = in.nextLine();

            // Ask for character info
            out.println("\nStatus Codes ~");
            out.println("\tNot In Word - r\n\tElsewhere - y\n\tCorrect - g");
            out.print("\nEnter status (ex: rgyry): ");
            String status = in.nextLine();

            out.println("\n*** Calculating ***\n");
            // Eliminate words using characters' hints
            eliminated = eliminateWords(word, status, wordsLeft);
            out.println(eliminated + " words eliminated.");
            printRemainingPossibilities(out, wordsLeft);
            if (wordsLeft.size() > 1) {
                printBestNextGuess(wordsLeft, out);
            }
            guessNum++;
        }

        /*
         * Test every possible word and words remaining Find average number of
         * guesses eliminated (1.0/wordsLeft.size * (# eliminated) Best word:
         * highest avg
         */

        // Print guesses used
        out.println("GUESSES NEEDED: " + guessNum);

        /*
         * Close input and output streams
         */
        allWords.close();
        in.close();
        out.close();
    }

}