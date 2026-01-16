package org.wecancodeit;

import static org.junit.jupiter.api.Assertions.*;


import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wecancodeit.backend.BOService.AnimalQuestionService;
import org.wecancodeit.backend.DataModels.AnimalQuestionModel;
import org.wecancodeit.backend.Enums.DifficultyLevel;

public class AnimalQuestionServiceTest {

    private AnimalQuestionService animalQuestionService;

    @BeforeEach
    void setUp() {
        animalQuestionService = new AnimalQuestionService();
    }

     @Test
    void testGetAllAnimalQuestions_ReturnsNonEmptyList() {
        List<AnimalQuestionModel> questions = animalQuestionService.getAllAnimalQuestions();
        assertNotNull(questions, "The list of questions should not be null");
        assertFalse(questions.isEmpty(), "The list of questions should not be empty");
    }

    @Test
    void testGetAllAnimalQuestions_ContainsExpectedNumberOfQuestions() {
        List<AnimalQuestionModel> questions = animalQuestionService.getAllAnimalQuestions();
        assertEquals(39, questions.size(), "The list should contain 39 predefined questions");
    }

    @Test
    void testGetRandomAnimalQuestion_ReturnsQuestionForBeginnerLevel() {
        AnimalQuestionModel questionBeginner = animalQuestionService.getRandomAnimalQuestion(DifficultyLevel.Beginner);
        assertNotNull(questionBeginner, "A question should be returned for Beginner level");
        assertEquals(DifficultyLevel.Beginner, questionBeginner.getDifficulty(), "The difficulty of the question should be Beginner");
    }

    @Test
    void testGetRandomAnimalQuestion_ReturnsQuestionForIntermediateLevel() {
        AnimalQuestionModel question = animalQuestionService.getRandomAnimalQuestion(DifficultyLevel.Intermediate);

        assertNotNull(question, "A question should be returned for Intermediate level");
        assertEquals(DifficultyLevel.Intermediate, question.getDifficulty(), "The difficulty of the question should be Intermediate");
    }

    @Test
    void testGetRandomAnimalQuestion_ReturnsQuestionForAdvancedLevel() {
        AnimalQuestionModel question = animalQuestionService.getRandomAnimalQuestion(DifficultyLevel.Advanced);
        assertNotNull(question, "A question should be returned for Advanced level");
        // assertEquals(DifficultyLevel.Advanced, question.getDifficulty(), "The difficulty of the question should be Advanced");
    }

   
}