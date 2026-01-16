package service;

import model.Epic;
import model.Subtask;
import model.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class FileBackedTaskManagerTest {
    File tmpFile;
    BufferedWriter file;
    ManagerSaveException thrown;

    @BeforeEach
    void beforeEach() throws IOException {
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
    }

    @Test
    void shouldBeNullWhenFileIsNotExist() {
        FileBackedTaskManager fileBackedTaskManager = FileBackedTaskManager.loadFromFile("tmp.csv");
        assertNull(fileBackedTaskManager, "Объект FileBackedTaskManager не должен быть создан если нет указанного " +
                "файла.");
    }

    @Test
    void shouldBeExceptionWhenFileIsEmpty() throws IOException {
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Должно быть исключение с текстом: Файл пустой!");
    }

    @Test
    void saveToFile() throws IOException {
        File tmpFile2 = File.createTempFile("tasks", null);
        FileBackedTaskManager taskManager = new FileBackedTaskManager(tmpFile2.toString());
        Task task;
        Epic epic;
        Subtask subtask;

        // создадим две задачи
        task = new Task("Почистить ковер", "Отвезти в химчистку Ковер-33");
        taskManager.addTask(task); // id будет = 1
        task = new Task("Сварить борщ", "Найти рецепт борща");
        taskManager.addTask(task); // id будет = 2

        // создадим эпик с тремя подзадачами
        epic = new Epic("Переезд", "Переезд на новую квартиру");
        taskManager.addEpic(epic); // id будет = 3
        subtask = new Subtask(epic, "Грузчики", "Найти грузчиков");
        taskManager.addSubtask(subtask); // id будет = 4
        subtask = new Subtask(epic, "Кот", "Поймать кота и упаковать");
        taskManager.addSubtask(subtask); // id будет = 5
        subtask = new Subtask(epic, "Мебель", "Запаковать мебель");
        taskManager.addSubtask(subtask); // id будет = 6

        // создадим файл CSV с таким же содержимым
        file.write("type,id,name,description,status,epic\n");
        file.write("TASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.write("TASK,2,Сварить борщ,Найти рецепт борща,NEW,\n");
        file.write("EPIC,3,Переезд,Переезд на новую квартиру,NEW,\n");
        file.write("SUBTASK,4,Грузчики,Найти грузчиков,NEW,3\n");
        file.write("SUBTASK,5,Кот,Поймать кота и упаковать,NEW,3\n");
        file.write("SUBTASK,6,Мебель,Запаковать мебель,NEW,3\n");
        file.close();

        // сравним файлы, они должны быть идентичны
        try {
            assertEquals(-1L, Files.mismatch(Paths.get(tmpFile2.toString()), Paths.get(tmpFile.toString())),
                    "Ошибка сохранения в файл!");
        } catch (IOException e) {
            System.out.println("Ошибка чтения файла!");
        }
    }

    @Test
    void loadFromFile() throws IOException {
        file.write("type,id,name,description,status,epic\n");
        file.write("TASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.write("TASK,2,Сварить борщ,Найти рецепт борща,NEW,\n");
        file.write("EPIC,3,Переезд,Переезд на новую квартиру,NEW,\n");
        file.write("EPIC,7,Помыть окна,Помыть окна после зимы,NEW,\n");
        file.write("SUBTASK,4,Грузчики,Найти грузчиков,NEW,3\n");
        file.write("SUBTASK,5,Кот,Поймать кота и упаковать,NEW,3\n");
        file.write("SUBTASK,6,Мебель,Запаковать мебель,NEW,3\n");
        file.close();
        FileBackedTaskManager fileBackedTaskManager = FileBackedTaskManager.loadFromFile(tmpFile.toString());
        assertNotNull(fileBackedTaskManager, "Объект FileBackedTaskManager не создан.");
        assertEquals(2, fileBackedTaskManager.getTasks().size(), "Не загрузились задачи.");
        assertEquals(2, fileBackedTaskManager.getEpics().size(), "Не загрузились эпики.");
        assertEquals(3, fileBackedTaskManager.getSubtasks().size(), "Не загрузились подзадачи.");
    }

    @Test
    void generateExceptionManagerSaveException() throws IOException {
        // нет заголовка файла CSV
        file.write("TASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Должно быть исключение с текстом: Поврежден заголовок файла CSV: " +
                "неизвестное поле");

        // пропущено поле в заголовке файла CSV
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,,description,status,epic\n");
        file.write("TASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Должно быть исключение с текстом: Поврежден заголовок файла CSV: " +
                "неизвестное поле!");

        // не все поля в заголовке файла CSV
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,description,status,epic\n");
        file.write("TASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Должно быть исключение с текстом: не хватает полей!");

        // дублируются поля в заголовке
        // не должно быть исключения
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,type,id,name,description,status,epic\n");
        file.close();
        assertDoesNotThrow(() -> FileBackedTaskManager.loadFromFile(tmpFile.toString()), "Исключения быть не должно!");

        // не корректно указываем ID задачи
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,description,status,epic\n");
        file.write("TASK,один,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Строка 2: ID задачи должен быть числом!");

        // указываем ID задачи меньше нуля
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,description,status,epic\n");
        file.write("TASK,-100,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Строка 2: ID задачи должен быть больше нуля!");

        // указываем ID задачи = 0
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,description,status,epic\n");
        file.write("TASK,0,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Строка 2: ID задачи должен быть больше нуля!");

        // портим 2-ю строку файла: меняем тип задачи на не существующий
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,name,description,status,epic\n");
        file.write("T1ASK,1,Почистить ковер,Отвезти в химчистку Ковер-33,NEW,\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Должно быть исключение с текстом: Строка 2: не корректно указан тип " +
                "задачи!");

        // не корректно указываем номер эпика в подзадаче
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,name,description,status,epic\n");
        file.write("EPIC,3,Переезд,Переезд на новую квартиру,NEW,\n");
        file.write("SUBTASK,4,Грузчики,Найти грузчиков,NEW,3\n");
        file.write("SUBTASK,5,Кот,Поймать кота и упаковать,NEW,ТРИ\n");
        file.write("SUBTASK,6,Мебель,Запаковать мебель,NEW,3\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "Строка 4: ID эпика подзадачи должен быть числом!");

        // указываем отрицательный номер эпика в подзадаче
        tmpFile = File.createTempFile("tasks", null);
        file = new BufferedWriter(new FileWriter(tmpFile));
        file.write("type,id,name,description,status,epic\n");
        file.write("EPIC,3,Переезд,Переезд на новую квартиру,NEW,\n");
        file.write("SUBTASK,4,Грузчики,Найти грузчиков,NEW,3\n");
        file.write("SUBTASK,5,Кот,Поймать кота и упаковать,NEW,-3\n");
        file.write("SUBTASK,6,Мебель,Запаковать мебель,NEW,3\n");
        file.close();
        thrown = assertThrows(ManagerSaveException.class, () -> FileBackedTaskManager.loadFromFile(tmpFile.toString()));
        assertNotNull(thrown.getMessage(), "ID эпика подзадачи должен быть больше нуля!");
    }
}