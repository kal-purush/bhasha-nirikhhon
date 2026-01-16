package seedu.address.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class JsonCourseParserTest {
    private static final Path TEST_DATA_FOLDER = Paths.get("src", "test", "data", "JsonModuleListTest",
            "SimplifiedModuleList.json");

    @TempDir
    public Path testFolder;

    @Test
    public void parseJsonFile_validJsonFile_success() throws IOException {
        List<String> moduleCodes = JsonCourseParser.parseJsonFile(TEST_DATA_FOLDER);
        assertEquals(3, moduleCodes.size());
        assertEquals("CS2103T", moduleCodes.get(0));
        assertEquals("CS2040", moduleCodes.get(1));
        assertEquals("CS1101S", moduleCodes.get(2));
    }

    @Test
    public void parseJsonFile_missingFile_exceptionThrown() {
        Path filePath = Paths.get("nonExistentFile.json");
        assertThrows(IOException.class, () -> JsonCourseParser.parseJsonFile(filePath));
    }

    @Test
    public void parseJsonFile_invalidJsonFormat_exceptionThrown() {
        Path filePath = Paths.get("src", "test", "data", "JsonModuleListTest", "InvalidModuleList.json");
        assertThrows(IOException.class, () -> JsonCourseParser.parseJsonFile(filePath));
    }
}