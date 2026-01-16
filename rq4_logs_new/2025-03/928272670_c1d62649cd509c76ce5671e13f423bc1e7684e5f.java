import manager.TaskManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import task.Epic;
import task.Subtask;
import task.Task;
import task.TaskStatus;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

abstract class TaskManagerTest<M extends TaskManager> {

    protected M taskManager;

    abstract M getTaskManager();

    @BeforeEach
    void setUp() {
        taskManager = getTaskManager();
    }


    @Test
    void testAddTask() {
        Task task = new Task();
        task.setTitle("Заголовок");
        task.setStatus(TaskStatus.NEW);

        taskManager.addTask(task);
        Task savedTask = taskManager.getTask(task.getId());

        assertNotNull(savedTask, "Задача должна быть добавлена");
        assertEquals(task, savedTask, "Сохраненная задача должна соответствовать начальной");
    }

    @Test
    void testAddEpicAndSubtask() {
        Epic epic = new Epic();
        epic.setTitle("Эпик 1");

        taskManager.addEpic(epic);
        Subtask subtask = new Subtask();
        subtask.setTitle("Подзадача 1");
        subtask.setEpicId(epic.getId());

        taskManager.addSubtask(subtask);

        assertTrue(taskManager.getSubtasksByEpic(epic.getId()).contains(subtask),
                "Подзадача должена быть связана с Epic");
    }

    @Test
    void testRemoveTask() {
        Task task = new Task();
        task.setTitle("Заголовок");
        taskManager.addTask(task);
        int id = task.getId();

        taskManager.removeTask(id);

        assertNull(taskManager.getTask(id), "Задача должна быть удалена");
    }

    @Test
    void testRemoveSubtaskAndIdFromEpic() {
        Epic epic = new Epic();
        taskManager.addEpic(epic);

        Subtask subtask = new Subtask();
        subtask.setEpicId(epic.getId());
        taskManager.addSubtask(subtask);

        taskManager.removeSubtask(subtask.getId());

        assertFalse(taskManager.getSubtasksByEpic(epic.getId()).contains(subtask),
                "Подзадача должна быть удалена из Epic");
    }

    @Test
    void testUpdateEpicStatusWhenSubtaskChanges() {
        Epic epic = new Epic();
        taskManager.addEpic(epic);

        Subtask subtask = new Subtask();
        subtask.setEpicId(epic.getId());
        subtask.setStatus(TaskStatus.NEW);
        taskManager.addSubtask(subtask);

        subtask.setStatus(TaskStatus.DONE);
        taskManager.updateSubtask(subtask);

        assertEquals(TaskStatus.DONE, epic.getStatus(),
                "Статус эпика должен быть обновлен на Done");
    }

    @Test
    void testTaskIntersection() {
        Task task1 = new Task();
        task1.setTitle("Задача 1");
        task1.setStartTime(LocalDateTime.now());
        task1.setDuration(Duration.ofHours(1));
        taskManager.addTask(task1);

        Task task2 = new Task();
        task2.setTitle("Задача 2");
        task2.setStartTime(LocalDateTime.now().plusMinutes(30));
        task2.setDuration(Duration.ofHours(1));

        assertThrows(IllegalArgumentException.class, () -> taskManager.addTask(task2),
                "При наложении задач должно быть исключение");
    }

    @Test
    void testGetPrioritizedTasks() {
        Task task1 = new Task();
        task1.setTitle("Задача 1");
        task1.setStartTime(LocalDateTime.now());
        task1.setDuration(Duration.ofHours(1));
        taskManager.addTask(task1);

        Task task2 = new Task();
        task2.setTitle("Задача 2");
        task2.setStartTime(LocalDateTime.now().plusHours(2));
        task2.setDuration(Duration.ofHours(1));
        taskManager.addTask(task2);

        List<Task> prioritizedTasks = taskManager.getPrioritizedTasks();
        assertEquals(2, prioritizedTasks.size(), "Должно быть 2 задачи");
        assertEquals(task1, prioritizedTasks.get(0), "Первая задача - задачч 1");
        assertEquals(task2, prioritizedTasks.get(1), "Вторая задача - задача 2");
    }
}