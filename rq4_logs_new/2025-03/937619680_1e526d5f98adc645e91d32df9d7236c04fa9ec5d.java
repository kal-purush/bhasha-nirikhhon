package com.gowri.blitz.service.impl;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * @author NaveenWodeyar
 * @date 09-03-2025
 */

public class DuplicateElementsTest {

    @Test
    public void testFindDuplicates() {
        // Prepare the expected output for the given array.
        int[] inputArray = {1, 2, 3, 4, 5, 2, 3, 6, 7};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string and split by newlines
        String output = outputStream.toString();
        String[] lines = output.split(System.lineSeparator());

        // Check if the output contains the expected duplicate values.
        List<String> expectedDuplicates = List.of("2", "3");

        // Check the printed output against expected duplicates
        for (String expected : expectedDuplicates) {
            boolean containsExpected = false;
            for (String line : lines) {
                if (line.contains(expected)) {
                    containsExpected = true;
                    break;
                }
            }
            assertTrue(containsExpected, "Expected duplicate not found in output: " + expected);
        }

        // You can also assert that no other numbers are printed as duplicates
        for (String line : lines) {
            if (!expectedDuplicates.contains(line.trim())) {
                assertFalse(line.trim().matches("\\d+"), "Unexpected output found: " + line);
            }
        }
    }

    @Test
    public void testFindDuplicates1() {
        // Prepare the expected output for the given array.
        int[] inputArray = {1, 2, 3, 4, 5, 2, 3, 6, 7};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string and split by newlines
        String output = outputStream.toString();
        String[] lines = output.split(System.lineSeparator());

        // Check if the output contains the expected duplicate values.
        List<String> expectedDuplicates = List.of("2", "3");

        // Check the printed output against expected duplicates
        for (String expected : expectedDuplicates) {
            boolean containsExpected = false;
            for (String line : lines) {
                if (line.contains(expected)) {
                    containsExpected = true;
                    break;
                }
            }
            assertTrue(containsExpected, "Expected duplicate not found in output: " + expected);
        }

        // You can also assert that no other numbers are printed as duplicates
        for (String line : lines) {
            if (!expectedDuplicates.contains(line.trim())) {
                assertFalse(line.trim().matches("\\d+"), "Unexpected output found: " + line);
            }
        }
    }

    @Test
    public void testFindNoDuplicates() {
        // Array with no duplicates
        int[] inputArray = {1, 2, 3, 4, 5, 6, 7};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string
        String output = outputStream.toString();

        // Assert that no duplicates are printed
        assertFalse(output.contains("duplicate"), "Unexpected duplicates found when there should be none.");
    }

    @Test
    public void testFindSingleElement() {
        // Array with only one element
        int[] inputArray = {1};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string
        String output = outputStream.toString();

        // Assert that no duplicates are printed
        assertFalse(output.contains("duplicate"), "Unexpected duplicates found when there should be none.");
    }

    @Test
    public void testFindAllDuplicates() {
        // Array where all elements are duplicates
        int[] inputArray = {2, 2, 2, 2, 2};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string
        String output = outputStream.toString();

        // Assert that the only duplicate printed is '2'
        assertTrue(output.contains("2"), "Expected duplicate not found in output: 2");
    }

    @Test
    public void testFindEmptyArray() {
        // Empty array
        int[] inputArray = {};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string
        String output = outputStream.toString();

        // Assert that no duplicates are printed
        assertFalse(output.contains("duplicate"), "Unexpected duplicates found when the array is empty.");
    }

    @Test
    public void testFindDuplicateFailure() {
        // Array with expected failure scenario (for example, wrong array)
        int[] inputArray = {5, 6, 7, 8, 9, 10};

        // Capture the output printed to the console
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);

        // Call the findDuplicates method
        DuplicateElements.findDuplicates(inputArray);

        // Convert the captured output to a string
        String output = outputStream.toString();

        // Assert that the output does not contain any duplicates
        assertTrue(output.isEmpty(), "Expected no duplicates, but got: " + output);
    }

}