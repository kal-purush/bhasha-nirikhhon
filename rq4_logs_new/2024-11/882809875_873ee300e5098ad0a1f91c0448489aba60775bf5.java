package org.wecancodeit.backend;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wecancodeit.backend.BOService.ColorService;
import org.wecancodeit.backend.DataModels.ColorModel;
import org.wecancodeit.backend.Enums.DifficultyLevel;

public class ColorServiceTest {
    
    private ColorService colorService;

    @BeforeEach
    void setUp() {
        colorService = new ColorService();
    }

    @Test
    void testGetAllQuestions(){
        List<ColorModel> allQuestions = colorService.getAllQuestions();

        assertNotNull(allQuestions, "The complete list of questions should not be null");
        assertEquals(60, allQuestions.size(), "The list of questions should contain exactly 60 questions");
    }

    @Test
    void testGetRandomQuestionsByDifficulty_Beginner(){
        ColorModel question = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Beginner);

        assertNotNull(question, "The question should not be null, as it is returned from the Beginner questions");
        assertEquals(DifficultyLevel.Beginner, question.getDifficulty());
    }

    @Test
    void testGetRandomQuestionsByDifficulty_Intermediate(){
        ColorModel question = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Intermediate);

        assertNotNull(question, "The question should not be null, as it is returned from the Intermediate questions");
        assertEquals(DifficultyLevel.Intermediate, question.getDifficulty());
    }

    @Test
    void testGetRandomQuestionsByDifficulty_Advanced(){
        ColorModel question = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Advanced);

        assertNotNull(question, "The question should not be null, as it is returned from the Advanced questions");
        assertEquals(DifficultyLevel.Advanced, question.getDifficulty());
    }

    @Test
    void testGetRandomQuestionsByDifficulty_NoQuestionsForDifficulty(){
        colorService.getAllQuestions().clear();

        ColorModel questionBeginner = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Beginner);
        assertNull(questionBeginner, "No question should be returned when there are no questions available for the beginner difficulty");

        ColorModel questionIntermediate = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Intermediate);
        assertNull(questionIntermediate, "No question should be returned when there are no questions available for the intermediate difficulty");

        ColorModel questionAdvanced = colorService.getRandomQuestionByDifficulty(DifficultyLevel.Advanced);
        assertNull(questionAdvanced, "No question should be returned when there are no questions available for the advanced difficulty");
    }

    @Test
    void testUniqueQuestionIds(){
        List<ColorModel> allQuestions = colorService.getAllQuestions();
        Set<Long> uniqueIds = new HashSet<>();

        for (ColorModel question : allQuestions) {
            assertTrue(uniqueIds.add(question.getId()), "Question ID " + question.getId() + " is not unique");
        }
    }

    @Test
    void testSpecificBeginnerQuestion() {
        ColorModel question = null;
        List<String> expectedChoices = Arrays.asList("Green", "Blue", "Red", "Yellow");

        for (ColorModel q : colorService.getAllQuestions()) {
            if (q.getId().equals(1L) && q.getDifficulty() == DifficultyLevel.Beginner) {
                question = q;
                break;
            }
        }

        assertNotNull(question, "Expected Beginner question with ID 1L");
        assertEquals(1L, question.getId());
        assertEquals("What color is the sky on a clear day?", question.getQuestionText());
        assertEquals(DifficultyLevel.Beginner, question.getDifficulty());
        assertEquals(expectedChoices, question.getAnswerChoices(), "Expected answer choices are " + expectedChoices);
        assertEquals("Blue", question.getAnswerChoices().get(question.getAnswer()), "Expected answer is 'Blue'");
    }

    @Test
    void testSpecificIntermediateQuestion() {
        ColorModel question = null;
        List<String> expectedChoices = Arrays.asList("Purple", "Green", "Orange", "Brown");

        for (ColorModel q : colorService.getAllQuestions()) {
            if (q.getId().equals(21L) && q.getDifficulty() == DifficultyLevel.Intermediate) {
                question = q;
                break;
            }
        }

        assertNotNull(question, "Expected Intermediate question with ID 21L");
        assertEquals(21L, question.getId());
        assertEquals("What color do you get when you mix red and blue?", question.getQuestionText());
        assertEquals(DifficultyLevel.Intermediate, question.getDifficulty());
        assertEquals(expectedChoices, question.getAnswerChoices(), "Expected answer choices are " + expectedChoices);
        assertEquals("Purple", question.getAnswerChoices().get(question.getAnswer()), "Expected answer is 'Purple'");
    }

    @Test
    void testSpecificAdvancedQuestion() {
        ColorModel question = null;
        List<String> expectedChoices = Arrays.asList("Brown", "Black", "Gray", "White");
        
        for (ColorModel q : colorService.getAllQuestions()) {
            if (q.getId().equals(41L) && q.getDifficulty() == DifficultyLevel.Advanced) {
                question = q;
                break;
            }
        }

        assertNotNull(question, "Expected Advanced question with ID 41L");
        assertEquals(41L, question.getId());
        assertEquals("What color is produced when you mix all primary colors of paint?", question.getQuestionText());
        assertEquals(DifficultyLevel.Advanced, question.getDifficulty());
        assertEquals(expectedChoices, question.getAnswerChoices(), "Expected answer choices are " + expectedChoices);
        assertEquals("Brown", question.getAnswerChoices().get(question.getAnswer()), "Expected answer is 'Brown'");
    }


}