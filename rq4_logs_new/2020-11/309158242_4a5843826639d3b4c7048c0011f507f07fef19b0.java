package org.otus.algo;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMain {

    private static final String TMPL = "%s/test.%s.%s";
    private static final String ASSERTION_MSG_TMPL = "%s must be equal to %s \n Execution time: %s ms";

    public static void runTestsAndCheckResult(String testsPath, Function<String, String> func, int count,
                                              int maxCharsToCompare //should be used in case of rounding problems
    ) throws Exception {
        if (count == 0) {
            try (Stream<Path> paths = Files.walk(Paths.get(testsPath))) {
                count = (int) paths
                        .filter(p -> Files.isRegularFile(p) && FilenameUtils.isExtension(p.toString(), "in"))
                        .count();
            }
        }

        for (int i = 0; i < count; i++) {
            File inFile = Paths.get(String.format(TMPL, testsPath, i, "in")).toFile();
            String inData = FileUtils.readFileToString(inFile, StandardCharsets.UTF_8);
            File outFile = Paths.get(String.format(TMPL, testsPath, i, "out")).toFile();
            String expectedResult = FileUtils.readFileToString(outFile, "UTF-8").trim();

            System.out.println("Test " + i + " started");
            long startTime = System.currentTimeMillis();
            String executionResult = func.apply(inData);
            long endTime = System.currentTimeMillis();
            expectedResult = truncateIfNeeded(maxCharsToCompare, expectedResult);
            executionResult = truncateIfNeeded(maxCharsToCompare, executionResult);
            System.out.println("expected: " + expectedResult);
            System.out.println("execution result: " + executionResult);
            assertEquals(expectedResult, executionResult,
                    String.format(ASSERTION_MSG_TMPL, executionResult, expectedResult, endTime - startTime));
            if (executionResult.equals(expectedResult)) {
                System.out.println("Test " + i + " passed!");
            } else {
                System.out.println("Test " + i + " failed!");
                System.out.println(executionResult + " is not equal to " + expectedResult);
            }
            System.out.println("Execution time: " + (endTime - startTime) + " ms");
        }
    }

    private static String truncateIfNeeded(int maxCharsToCompare, String data) {
        return maxCharsToCompare > 0 && data.length() >= maxCharsToCompare ?
                data.substring(0, maxCharsToCompare) : data;
    }

    public static List<String> parseArgs(String input) {
        String[] args = input.split("\\r?\\n");
        return Arrays.asList(args);
    }
}