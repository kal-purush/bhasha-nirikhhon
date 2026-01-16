package seedu.data;

import seedu.task.DateSorter;
import seedu.task.PrioritySorter;
import seedu.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TaskList {

    private ArrayList<Task> tasks;

    public TaskList() {
        tasks = new ArrayList<>();
    }

    public void add(Task task) {
        tasks.add(task);
    }

    public int size() {
        return tasks.size();
    }

    public Task get(int index) {
        return tasks.get(index);
    }

    public TaskList sortListByDate() {
        // Sort by date
        List<Task> sorted = tasks.stream()
                .sorted(new DateSorter())
                .collect(Collectors.toList());
        return (TaskList) sorted;
    }

    public TaskList sortListByPriority() {
        // Sort by priority, same priority then date
        List<Task> sorted = tasks.stream()
                .sorted(new DateSorter())
                .sorted(new PrioritySorter())
                .collect(Collectors.toList());
        return (TaskList) sorted;
    }

}