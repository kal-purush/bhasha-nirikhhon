// MathTests.java
package org.wecancodeit.backend;

// Import necessary classes for assertions and testing
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.wecancodeit.backend.BOService.MathProblemService;
import org.wecancodeit.backend.DTOs.MathProblemDTO;
import org.wecancodeit.backend.Enums.DifficultyLevel;
import org.wecancodeit.backend.Enums.Operation;

public class MathTests {

    // Create an instance of MathProblemService to test its methods
    private final MathProblemService mathProblemService = new MathProblemService();

    @Test
    public void testAdditionOperation() {
        // Set up a MathProblemDTO with the Addition operation
        MathProblemDTO problem = new MathProblemDTO();
        problem.setOperation(Operation.Addition);
        problem.setNumerator(10); // Set numerator to 10
        problem.setDenominator(5); // Set denominator to 5

        // Calculate the answer using the MathProblemService
        float result = mathProblemService.getAnswer(problem);

        // Assert that the result is the sum of the numerator and denominator (10 + 5 =
        // 15)
        assertEquals(15, result, "The addition operation should return the sum of numerator and denominator.");
    }

    @Test
    public void testSubtractionOperation() {
        // Set up a MathProblemDTO with the Subtraction operation
        MathProblemDTO problem = new MathProblemDTO();
        problem.setOperation(Operation.Subtraction);
        problem.setNumerator(13); // Set numerator to 13
        problem.setDenominator(12); // Set denominator to 12

        // Calculate the answer using the MathProblemService
        float result = mathProblemService.getAnswer(problem);

        // Assert that the result is the difference between numerator and denominator
        // (13 - 12 = 1)
        assertEquals(1, result,
                "The subtraction operation should return the difference of numerator minus denominator.");
    }

    @Test
    public void testMultiplicationOperation() {
        // Set up a MathProblemDTO with the Multiplication operation
        MathProblemDTO problem = new MathProblemDTO();
        problem.setOperation(Operation.Multiplication);
        problem.setNumerator(10); // Set numerator to 10
        problem.setDenominator(10); // Set denominator to 10

        // Calculate the answer using the MathProblemService
        float result = mathProblemService.getAnswer(problem);

        // Assert that the result is the product of the numerator and denominator (10 *
        // 10 = 100)
        assertEquals(100, result,
                "The multiplication operation should return the product of numerator and denominator.");
    }

    @Test
    public void testDivisionOperation() {
        // Set up a MathProblemDTO with the Division operation
        MathProblemDTO problem = new MathProblemDTO();
        problem.setOperation(Operation.Division);
        problem.setNumerator(100); // Set numerator to 100
        problem.setDenominator(4); // Set denominator to 4

        // Calculate the answer using the MathProblemService
        float result = mathProblemService.getAnswer(problem);

        // Assert that the result is the quotient of the numerator divided by the
        // denominator (100 / 4 = 25)
        assertEquals(25, result,
                "The division operation should return the quotient of numerator divided by denominator.");
    }

    @Test
    public void testGetRandomOperationForBeginner() {
        // Define the expected set of operations for Beginner level
        Set<Operation> expectedOperations = EnumSet.of(Operation.Addition, Operation.Subtraction);

        // Run the random operation generation multiple times to verify it only produces
        // the allowed operations
        for (int i = 0; i < 100; i++) { // Run the test multiple times to account for randomness
            Operation operation = mathProblemService.getRandomOperation(DifficultyLevel.Beginner);
            assertTrue(expectedOperations.contains(operation),
                    "Beginner level should only produce Addition or Subtraction operations.");
        }
    }

    @Test
    public void testGetRandomOperationForIntermediate() {
        // Define the expected set of operations for Intermediate level
        Set<Operation> expectedOperations = EnumSet.of(Operation.Addition, Operation.Subtraction,
                Operation.Multiplication, Operation.Division);

        // Run the random operation generation multiple times to verify it includes all
        // allowed operations
        for (int i = 0; i < 100; i++) { // Run the test multiple times to account for randomness
            Operation operation = mathProblemService.getRandomOperation(DifficultyLevel.Intermediate);
            assertTrue(expectedOperations.contains(operation),
                    "Intermediate level should produce any of Addition, Subtraction, Multiplication, or Division operations.");
        }
    }

    @Test
    public void testGetRandomOperationForAdvanced() {
        // Define the expected set of operations for Advanced level
        Set<Operation> expectedOperations = EnumSet.of(Operation.Addition, Operation.Subtraction,
                Operation.Multiplication, Operation.Division);

        // Run the random operation generation multiple times to verify it includes all
        // allowed operations
        for (int i = 0; i < 100; i++) { // Run the test multiple times to account for randomness
            Operation operation = mathProblemService.getRandomOperation(DifficultyLevel.Advanced);
            assertTrue(expectedOperations.contains(operation),
                    "Advanced level should produce any of Addition, Subtraction, Multiplication, or Division operations.");
        }
    }
}