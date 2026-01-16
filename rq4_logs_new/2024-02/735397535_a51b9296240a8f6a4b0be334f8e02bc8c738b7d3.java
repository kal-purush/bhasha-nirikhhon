package manager;

import model.Epic;
import model.SubTask;
import model.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileBackedTaskManagerTest {
    protected FileBackedTaskManager fileManager;
    protected Path tempFile;

    @BeforeEach
    void beforeEach() throws IOException {
        try {
            tempFile = Files.createTempFile("test-", ".tmp");
            fileManager = new FileBackedTaskManager(Managers.getDefaultHistory(), tempFile);
        } catch (IOException e) {
            throw new IOException("Произошла ошибка создания файла");
        }
    }

    @Test
    void shouldSaveAndLoadIsEmptyFile() throws IOException {
        fileManager.save();
        assertTrue(Files.exists(tempFile), "Файл для записи данных не был создан");

        try {
            String contentsOfTheFile = Files.readString(tempFile);
            String[] split = contentsOfTheFile.split(",");
            assertEquals(6, split.length, "После сохранения пустого менеджера в файле должен быть только заголовок");
            FileBackedTaskManager fileManagerTest = FileBackedTaskManager.loadFromFile(tempFile);
            assertNotNull(fileManagerTest, "Загрузка из пустого файла не сработала");
            assertTrue(fileManagerTest.getAllTasks().isEmpty(), "После загрузки из пустого файла список задач должен быть пустым");
            assertTrue(fileManagerTest.getAllEpics().isEmpty(), "После загрузки из пустого файла список эпиков должен быть пустым");
            assertTrue(fileManagerTest.getAllSubTasks().isEmpty(), "После загрузки из пустого файла список подзадач должен быть пустым");
            assertTrue(fileManagerTest.getHistory().isEmpty(), "После загрузки из пустого файла история должна отсутствовать");
        } catch (IOException e) {
            throw new IOException("Произошла ошибка чтения файла");
        }
    }

    @Test
    void shouldSaveSeveralTasksInFile() throws FileNotFoundException {
        Task task1 = fileManager.createTask(new Task(".Task1", "Task1_descr"));
        Epic epic1 = fileManager.createEpic(new Epic("Epic1", "Epic1_descr1"));
        Epic epic2 = fileManager.createEpic(new Epic("Epic2", "Epic1_descr2"));
        SubTask subTask1 = fileManager.createSubTask(new SubTask("ST1", "ST1_descr", 2));
        SubTask subTask2 = fileManager.createSubTask(new SubTask("ST2", "ST2_descr", 2));
        SubTask subTask3 = fileManager.createSubTask(new SubTask("ST3", "ST3_descr", 3));

        assertTrue(Files.exists(tempFile), "Файл для хранения данных не был создан");

        try (BufferedReader reader = new BufferedReader(new FileReader(tempFile.toString()))) {
            String line = reader.readLine();
            assertEquals("id,type,name,status,description,epic", line, "Первой строкой должен быть заголовок");
            for (int i = 1; i <= 6 ; i++) {
                String testLine = reader.readLine();
                if (i == 1) assertEquals(task1.toString(), testLine, "Второй строкой должна быть запись task1");
                if (i == 3) assertEquals(epic2.toString(), testLine, "Четвертой строкой должна быть запись subTask1");
                if (i == 6) assertEquals(subTask3.toString(), testLine, "");
            }
        } catch (IOException e) {
            throw new RuntimeException("Ошибка чтения из файла", e);
        }
    }

    @Test
    void shouldLoadFromFileSeveralTasksAndHistory() {
        Task task1 = fileManager.createTask(new Task(".Task1", "Task1_descr"));
        Epic epic1 = fileManager.createEpic(new Epic("Epic1", "Epic1_descr"));
        SubTask subTask1 = fileManager.createSubTask(new SubTask("ST1", "ST1_descr", 2));
        Epic epic2 = fileManager.createEpic(new Epic("Epic2", "Epic2_descr"));

        fileManager.getTaskById(1);
        fileManager.getEpicById(2);
        fileManager.getSubTaskById(3);

        FileBackedTaskManager fileManagerToTest = FileBackedTaskManager.loadFromFile(tempFile);
        assertNotNull(fileManagerToTest, "Ошибка при создании менеджера");
        assertEquals(1, fileManagerToTest.getAllTasks().size(), "Ошибка загрузки тасок");
        assertEquals(2, fileManagerToTest.getAllEpics().size(), "Ошибка загрузки эпиков");
        assertEquals(1, fileManagerToTest.getAllSubTasks().size(), "Ошибка загрузки сабтасок");
        assertEquals(3, fileManagerToTest.getHistory().size(), "Ошибка загрузки истории");
        assertEquals(3, fileManagerToTest.getHistory().getFirst().getId(),
                "Нарушен порядок выведения просмотренных задач в истории");


    }



}