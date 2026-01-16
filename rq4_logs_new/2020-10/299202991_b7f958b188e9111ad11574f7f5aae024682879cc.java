package seedu.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import seedu.task.Task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskListTest {
    private TaskList tasks;

    private Task meeting;
    private Task reading;
    private Task lecture;
    private Task tutorial;

    @BeforeEach
    public void setup() {
        tasks = new TaskList();
        meeting = new Task("meeting", "13-10-2020", "2000", "2");
        reading = new Task("reading", "21-10-2020", null, null);
        lecture = new Task("lecture", "20-10-2020", "1800", null);
        tutorial = new Task("tutorial", "19-10-2020", null, "1");
    }

    @Test
    void addTask() {
        tasks.addTask(meeting);
        tasks.addTask(reading);
        assertEquals(2, tasks.size());
        assertTrue(tasks.contains(meeting));
        assertFalse(tasks.contains(lecture));
        tasks.addTask(lecture);
        tasks.addTask(tutorial);
        assertEquals(4, tasks.size());
        assertTrue(tasks.contains(lecture));
        assertTrue(tasks.contains(tutorial));
    }

    @Test
    void sortListByDate() {
        addTask();
        TaskList sortedDate = tasks.sortListByDate();
        assertEquals(meeting, sortedDate.get(0));
        assertEquals(tutorial, sortedDate.get(1));
        assertEquals(lecture, sortedDate.get(2));
        assertEquals(reading, sortedDate.get(3));
    }

    @Test
    void sortListByPriority() {
        addTask();
        TaskList sortedPriority = tasks.sortListByPriority();
        assertEquals(meeting, sortedPriority.get(0));
        assertEquals(tutorial, sortedPriority.get(1));
        assertEquals(lecture, sortedPriority.get(2));
        assertEquals(reading, sortedPriority.get(3));
    }
}