package com._Nology_test;

import com._Nology.Question;
import com._Nology.QuizGame;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class QuizGame_test {

    @Test
    public void isValidAnswerWithAllowedAanswer_isValid() {
        // Arrange
        String selectedValidAnswer = "a";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedBanswer_isValid() {
        // Arrange
        String selectedValidAnswer = "b";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedCanswer_isValid() {
        // Arrange
        String selectedValidAnswer = "c";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedDanswer_isValid() {
        // Arrange
        String selectedValidAnswer = "d";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedAanswerToUpper_isValid() {
        // Arrange
        String selectedValidAnswer = "A";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedBanswerToUpper_isValid() {
        // Arrange
        String selectedValidAnswer = "B";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedCanswerToUpper_isValid() {
        // Arrange
        String selectedValidAnswer = "C";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithAllowedDanswerToUpper_isValid() {
        // Arrange
        String selectedValidAnswer = "D";
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());
        // Act
        Boolean result = myQuizGame.isValidAnswer(selectedValidAnswer);
        // Assert
        assertEquals(true, result);
    }

    @Test
    public void isValidAnswerWithNotAllowedLetters_isInvalid() {
        // Arrange
        String[] notAllowedLetters = {"e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());

        for (int index = 0; index < notAllowedLetters.length; index++) {
            // Act
            Boolean result = myQuizGame.isValidAnswer(notAllowedLetters[index]);
            // Assert
            assertEquals(false, result);
        }
    }


    @Test
    public void isValidAnswerWithNotAllowedNumbers_isInvalid() {
        // Arrange
        String[] notAllowedNumbers = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        QuizGame myQuizGame = new QuizGame("John", 0, new ArrayList<Question>());

        for (int index = 0; index < notAllowedNumbers.length; index++) {
            // Act
            Boolean result = myQuizGame.isValidAnswer(notAllowedNumbers[index]);
            // Assert
            assertEquals(false, result);
        }
    }
}