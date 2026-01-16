package org.wecancodeit.backend.BOService;

import java.util.*;

import org.springframework.stereotype.Service;
import org.wecancodeit.backend.DataModels.ColorModel;
import org.wecancodeit.backend.Enums.DifficultyLevel;

@Service
public class ColorService {
    
    private List<ColorModel> questions;

    public ColorService() {
        questions = new ArrayList<>();

        // The Hardcoded in questions with IDs to identify them by

        // ----------------------------Beginner--------------------------
        questions.add(new ColorModel(
            1L, // Unique ID for this question. "L" marks it as a long
            "What color do you get when you mix red and yellow?",
            Arrays.asList("Orange", "Green", "Purple", "Brown"),
            "Orange",
            "/images/colors/orange.jpg",
            DifficultyLevel.Beginner
        ));

        questions.add(new ColorModel(
            2L, 
            "What color do you get when you mix blue and yellow?",
            Arrays.asList("Green", "Purple", "Orange", "Gray"),
            "Green",
            "/images/colors/green.jpg",
            DifficultyLevel.Beginner
        ));
        // ----------------------------Intermediate--------------------------
        questions.add(new ColorModel(
            3L,
            "Which color is associated with calm and peace?",
            Arrays.asList("Red", "Blue", "Green", "Yellow"),
            "Blue",
            "/images/colors/blue.jpg",
            DifficultyLevel.Intermediate
        ));
        // -----------------------------Advanced-----------------------------
        questions.add(new ColorModel(
            4L,
            "What is the complementary color of green?",
            Arrays.asList("Red", "Orange", "Purple", "Blue"),
            "Red",
            "/images/colors/green-red.jpg",
            DifficultyLevel.Advanced
        ));
    }

    // The method to get all the questions
    public List<ColorModel> getAllQuestions() {
        return questions;
    }

    // The method to get a random question based on the difficulty
    public ColorModel getRandomQuestionByDifficulty(DifficultyLevel difficulty){
        // Step 1: Create a new list to store matching questions
        List<ColorModel> filteredQuestions = new ArrayList<>();
            // Step 2: Loop through each question in the main list
        for (ColorModel question : questions) {
            // Step 3: Check if the question's difficulty matches the specified level
            if (question.getDifficulty() == difficulty) {
                // Step 4: If it matches, add it to the filtered list
                filteredQuestions.add(question);
            }
        }

        // Now, the filteredQuestions has only the questions of the specific difficulty

        // Select and returns a random question from the filteredQuestions
        if (!filteredQuestions.isEmpty()) {
            Random random = new Random();
            int randomNumber = random.nextInt(filteredQuestions.size());
            return filteredQuestions.get(randomNumber);
        } else {
            return null; // No questions found for this difficulty
        }
    }

}