package com.epam.mjc.nio;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileReaderTest {

    private final static Path TEST_FILE = Path.of("src/test/resources/test.txt");
    private final static Path READER_CLASS = Path.of("src/main/java/com/epam/mjc/nio/FileReader.java");
    private final static String SOURCE_CODE_LOCATION = "src/main/java";
    private static Profile profile;

    @BeforeClass
    public static void setup() {
        String randomString = RandomStringUtils.randomAlphabetic(5);
        Integer randomInt = getRandomInt(1, 20);
        profile = new Profile(randomString, randomInt, randomString, randomInt.longValue());
        createTestFile(randomString, randomInt);
    }

    @AfterClass
    public static void clean() {
        try {
            Files.delete(TEST_FILE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testProgramReadsDataCorrectly () {
        FileReader fileReader = new FileReader();
        Profile actual = fileReader.getDataFromFile(new File(TEST_FILE.toUri()));
        assertEquals(profile, actual);
    }

    @Test
    public void testCodeWithoutExternalUtils() throws IOException {
        final Path sources = Path.of(SOURCE_CODE_LOCATION);
        Files.walk(sources)
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".java"))
                .forEach(this::assertSourceWithoutExternalUtils);
    }

    @Test
    public void testCodeHasStreamClosing() {
        String sourceCode = readFileIntoString(READER_CLASS);
        assertTrue("Code doesn't contain closing or try-with-resources",
                sourceCode.contains("try (") || sourceCode.contains("try(") || sourceCode.contains(".close()"));
    }
    @Test
    public void testCodeHasNio() {
        String sourceCode = readFileIntoString(READER_CLASS);
        assertTrue(sourceCode.contains(".nio"));
    }

    private void assertSourceWithoutExternalUtils(Path path) {
        String sourceCode = readFileIntoString(path);
        assertFalse("Code contains prohibited \"FileUtils\"", sourceCode.contains("FileUtils"));
        assertFalse("Code contains prohibited \"IOUtils\"", sourceCode.contains("IOUtils"));
        assertFalse("Code contains prohibited \"Scanner\"", sourceCode.contains("Scanner"));
        assertFalse("Code contains prohibited \"StreamTokenizer\"",sourceCode.contains("StreamTokenizer"));
        assertFalse("Code contains prohibited \"com.google\"", sourceCode.contains("com.google"));
    }

    private String readFileIntoString(Path sourcePath) {
        try {
            return Files.readString(sourcePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getRandomInt(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    public static void createTestFile(String randomString, Integer randomInt) {
        try {
            FileUtils.writeStringToFile(new File(TEST_FILE.toUri()),
                    "Name: " + randomString + System.lineSeparator() +
                            "Age: "  + randomInt + System.lineSeparator() +
                            "Email: " + randomString + System.lineSeparator() +
                            "Phone: "  + randomInt + System.lineSeparator(),
                    "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}