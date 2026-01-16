package seedu.data;

import org.junit.jupiter.api.Test;
import seedu.task.Task;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TaskMapTest {
    private TaskMap taskMap = new TaskMap();
    private Task t1;
    private Task t2;
    private Task t3;
    private Task t4;
    private Task t5;

    void setup() {
        try {
            t1 = new Task("meeting", "20-10-2020", null, null, null);
            t2 = new Task("meeting", "20-10-2020", null, null, "2");
            t3 = new Task("reading", "17-10-2020", null, null, "2");
            t4 = new Task("reading", "24-10-2020", "2200", "2300", null);
            t5 = new Task("random", "15-10-2020", null, null, "3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void operations_emptyTaskMap_success() {
        assertEquals(0, taskMap.size());
        assertNull(taskMap.get(Integer.parseInt("1234")));
        assertEquals(0, taskMap.getValues().size());
        assertEquals(0, taskMap.sortListByDate().size());
        assertEquals(0, taskMap.sortListByPriority().size());
        assertEquals(0, taskMap.searchByDate(LocalDate.now()).size());
        assertEquals(0, taskMap.searchByDescription("abc").size());

        // All sizes should be 0.
        int sizeBeforeDelete = taskMap.size();
        taskMap.delete(Integer.parseInt("1234"));
        assertEquals(sizeBeforeDelete, taskMap.size());

        int sizeBeforeClear = taskMap.size();
        taskMap.clear();
        assertEquals(sizeBeforeClear, taskMap.size());
    }

    @Test
    void operations_nonEmptyTaskMap_success() {
        setup();
        taskMap.addTask(t1);
        assertEquals(1, taskMap.size());
        assertEquals(t1, taskMap.get(t1.getTaskID()));

        taskMap.addTask(t2);
        int currentSize = taskMap.size();
        assertEquals(2, currentSize);
        // Found both tasks
        assertEquals(currentSize, taskMap.searchByDescription("meet").size());
        assertEquals(currentSize, taskMap.searchByDescription("Meet").size());
        assertEquals(currentSize, taskMap.searchByDate(LocalDate.now()).size());
        // Not found
        assertEquals(0, taskMap.searchByDate(LocalDate.of(2020, 10,30)).size());
        assertEquals(0, taskMap.searchByDescription("reading").size());

        taskMap.addTask(t3);
        taskMap.addTask(t4);
        currentSize = taskMap.size();
        assertEquals(currentSize, taskMap.searchByDescription("ing").size());

        taskMap.addTask(t5);

        // getValues()
        assertEquals(5, taskMap.getValues().size());

        // Sort by date
        TaskMap sortedByDate = taskMap.sortListByDate();
        Integer[] indexSortedByDate = {t5.getTaskID(), t3.getTaskID(),
                                       t1.getTaskID(), t2.getTaskID(), t4.getTaskID()};
        int count = 0;
        for (Task t : sortedByDate.getValues()) {
            assertEquals(taskMap.get(indexSortedByDate[count++]), t);
        }

        // Sort by priority
        TaskMap sortedByPriority = taskMap.sortListByPriority();
        Integer[] indexSortedByPriority = {t5.getTaskID(), t3.getTaskID(),
                                           t2.getTaskID(), t1.getTaskID(), t4.getTaskID()};
        count = 0;
        for (Task t : sortedByPriority.getValues()) {
            assertEquals(taskMap.get(indexSortedByPriority[count++]), t);
        }

        taskMap.delete(t1.getTaskID());
        assertEquals(4, taskMap.size());
        taskMap.clear();
        assertEquals(0, taskMap.size());
    }

    @Test
    void initialiseTaskMap_TaskList_success() {
        setup();
        List<Task> tasks = new ArrayList<>();
        tasks.add(t1);
        tasks.add(t2);
        tasks.add(t3);
        tasks.add(t4);
        tasks.add(t5);

        taskMap = new TaskMap(tasks);
        assertEquals(tasks.size(), taskMap.size());
    }

}