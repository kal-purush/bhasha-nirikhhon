import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;

public class Tier1tests {

    @Test
    void testAddSingleTask() {
        TaskManager manager = new TaskManager();
        manager.addTask("Complete Tier 1");
        List<String> tasks = manager.listTasks();

        assertEquals(1, tasks.size(), "Task list should have 1 task");
        assertTrue(tasks.contains("Complete Tier 1"), "Task list should include the added task");
    }

    @Test
    void testAddMultipleTasks() {
        TaskManager manager = new TaskManager();
        manager.addTask("Task 1");
        manager.addTask("Task 2");
        List<String> tasks = manager.listTasks();

        assertEquals(2, tasks.size(), "Task list should have 2 tasks");
        assertTrue(tasks.containsAll(List.of("Task 1", "Task 2")), "All tasks should be listed");
    }

    @Test
    void testListTasksInitiallyEmpty() {
        TaskManager manager = new TaskManager();
        List<String> tasks = manager.listTasks();
        assertTrue(tasks.isEmpty(), "New TaskManager should have an empty task list");
    }
}