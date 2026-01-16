package org.wecancodeit.backend.BOService;

// Importing required classes and enums
import org.wecancodeit.backend.DTOs.MathProblemDTO; // Data Transfer Object for math problems
import org.wecancodeit.backend.Enums.*; // Importing all enums in the package
import java.util.Random; // Random class for generating random numbers
import org.springframework.stereotype.Service; // Spring annotation to define a service class

@Service
public class MathProblemService {
    // Random instance for generating random numbers
    private Random random = new Random();

    // Method to generate a math problem based on the specified difficulty level
    public MathProblemDTO generateMathProblem(DifficultyLevel difficulty) {
        MathProblemDTO mathProblemDTO = new MathProblemDTO(); // Create a new MathProblemDTO instance
        mathProblemDTO.setDifficulty(difficulty); // Set the difficulty level of the problem
        mathProblemDTO.setOperation(getRandomOperation(difficulty)); // Set a random operation based on difficulty
        mathProblemDTO.setNumerator(getRandomNumber(difficulty, mathProblemDTO.getOperation())); // Set a random
                                                                                                 // numerator
        mathProblemDTO.setDenominator(getRandomNumber(difficulty, mathProblemDTO.getOperation())); // Set a random
                                                                                                   // denominator
        return mathProblemDTO; // Return the generated math problem
    }

    // Private method to get a random mathematical operation based on difficulty
    // level
    private Operation getRandomOperation(DifficultyLevel difficulty) {
        Operation result = Operation.Addition; // Default operation
        switch (difficulty) {
            case Beginner: { // For beginner level, allow only addition
                int i = random.nextInt(0, 1); // Randomly choose an operation index (0 for addition)
                result = Operation.values()[i]; // Set the operation
                break;
            }
            case Advanced:
            case Intermediate: { // For intermediate and advanced levels, allow multiple operations
                int i = random.nextInt(0, 3); // Randomly choose an operation index (0-2 for addition, subtraction,
                                              // multiplication)
                result = Operation.values()[i]; // Set the operation
                break;
            }
        }
        return result; // Return the randomly selected operation
    }

    // Private method to generate a random number based on difficulty level and
    // operation type
    private float getRandomNumber(DifficultyLevel difficulty, Operation operation) {
        float result = 0; // Initialize result
        switch (difficulty) {
            case Beginner: { // For beginner level, generate a number between 1 and 10
                result = random.nextInt(1, 10); // Generate a random integer
                break;
            }
            case Intermediate: { // For intermediate level, generate numbers based on operation
                if (operation == Operation.Addition || operation == Operation.Subtraction) {
                    result = random.nextInt(1, 50); // Generate random numbers for addition/subtraction
                } else {
                    result = random.nextInt(1, 10); // Generate smaller numbers for multiplication/division
                }
                break;
            }
            case Advanced: { // For advanced level, generate numbers based on operation
                if (operation == Operation.Addition || operation == Operation.Subtraction) {
                    result = random.nextInt(1, 100); // Generate larger numbers for addition/subtraction
                } else {
                    result = random.nextInt(1, 25); // Generate smaller numbers for multiplication/division
                }
                break;
            }
        }
        return result; // Return the generated random number
    }
}