package org.vashonsd.Games.BR;


import org.vashonsd.Utils.LetterStore;
import org.vashonsd.Utils.Randomizer;
import org.vashonsd.Utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Round {

    String goalWord;
    char[] goalWordArray;
    Boolean[] hits;
    List<Character> screenedWord;
    LetterStore lettersGuessed;
    boolean isOnlyLetters;
    int guesses;

    public Round() {
        try {
            setUpNewGame();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setUpNewGame() throws IOException {
        goalWord = Randomizer.randomWord();
        hits = new Boolean[goalWord.length()];
        for(int i=0; i<hits.length; i++) {hits[i] = false;}
        lettersGuessed = new LetterStore();
        goalWordArray = goalWord.toCharArray();
        isOnlyLetters = false;
        guesses = 10;
        screenedWord = new ArrayList<>();
        arrayListOfUnderscores();
    }
    /**
     * Call this method when the user supplies a char as a guess.
     * This method will add that char to the list of guessed letters, and refigure the
     * array of true/false.
     *
     * @param c
     */
    public void handleGuess(char c) {
        lettersGuessed.setTrue(c);
        for (int i = 0; i < goalWordArray.length; i++) {
            if (goalWordArray[i] == c) {
                hits[i] = true;
                screenedWord.set(i, c);
            }
        }
    }

    /**
     * takes the array of indexes and the arrayList of guesses, and the string that you are guessing and it adds the
     * string to the arrayList of correctGuesses
     * <p>
     * Example: addToArrayList([1,3,5], List{_ _ _ _ _ _}, "a") => "_ a _ a _ a"
     */
    public void arrayListOfUnderscores() {
        for (int i = 0; i < goalWord.length(); i++) {
            screenedWord.add('_');
        }
    }

    //checks if the screenedWord is full of the letters and not blank at all
    public boolean playerHasWon() {
        for (int i = 0; i < hits.length; i++) {
            if (hits[i] == false) {
                return false;
            }
        }
        return true;
    }

    public boolean isInWord(char c) {
        boolean result = false;

        for (int i = 0; i < goalWordArray.length; i++) {
            if (goalWordArray[i] == c) {
                result = true;
            }
        }

        return result;
    }



    public int getGuesses() {
        return guesses;
    }

    public List<Character> getScreenedWord() {
        return screenedWord;
    }

    public String getStatus() {
        return "your status is " + screenedWord;
    }
    public String getGoalWord() {
        return goalWord;
    }
}