package sunny;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StorageTest {

    @TempDir
    Path tempDir;

    private Storage storage;
    private Path tempFilePath;

    @BeforeEach
    void setUp() throws IOException {
        // Creating a temporary file in the temp directory
        tempFilePath = Files.createTempFile(tempDir, "taskslist", ".txt");
        storage = new Storage(tempFilePath.toString());
    }

    @Test
    void testWriteTodoTask() throws Exception {
        List<Task> taskList = new ArrayList<>();
        taskList.add(TaskCreator.create("todo test")); // Assuming constructor with description and isDone status
        storage.write(taskList);

        List<String> lines = readFileContent();
        assertEquals(1, lines.size());
        assertEquals("todo test|false", lines.get(0));
    }

    @Test
    void testWriteDeadlineTask() throws Exception {
        List<Task> taskList = new ArrayList<>();
        taskList.add(TaskCreator.create("deadline test /by 2024-09-20"));
        storage.write(taskList);

        List<String> lines = readFileContent();
        assertEquals(1, lines.size());
        assertEquals("deadline test /by 2024-09-20|false", lines.get(0));
    }

    @Test
    void testReadFileWithTasks() throws Exception {
        List<Task> ls = List.of(
                TaskCreator.create("todo test"),
                TaskCreator.create("deadline test /by 2024-09-20"),
                TaskCreator.create("event test /from 2024-09-20 /to 2024-09-21")
        );
        storage.write(ls);

        List<Task> tasks = storage.read();

        assertEquals(3, tasks.size());

        assertEquals("test", tasks.get(0).getDescription());

        assertEquals("test /by 2024-09-20", tasks.get(1).getDescription());

        assertEquals("test /from 2024-09-20 /to 2024-09-21", tasks.get(2).getDescription());
    }

    private List<String> readFileContent() {
        try {
            return Files.readAllLines(tempFilePath);
        } catch (IOException e) {
            fail("Error reading file content.");
            return new ArrayList<>();
        }
    }
}