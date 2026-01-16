package seedu.data;

import seedu.task.DateSorter;
import seedu.task.PrioritySorter;
import seedu.task.Task;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TaskMap {

    private final HashMap<Integer, Task> tasksMap;


    public TaskMap() {
        tasksMap = new HashMap<>();
    }

    public TaskMap(List<Task> tasks) {
        tasksMap = new HashMap<>();
        for (Task task : tasks) {
            tasksMap.put(task.getHashValue(), task);
        }
    }

    public Collection<Task> getValues() {
        return tasksMap.values();
    }

    public void addTask(Task task) {
        tasksMap.put(task.getHashValue(), task);
    }

    //    public void addTasks(TaskMap tasks) {
    //        tasksMap.putAll((Map<? extends Integer, ? extends Task>) tasks);
    //    }

    public int size() {
        return tasksMap.size();
    }

    public Task get(Integer key) {
        return tasksMap.get(key);
    }

    //    public boolean contains(Task task) {
    //        return tasksMap.contains(task);
    //    }

    public TaskMap sortListByDate() {
        // Sort by date
        List<Task> sorted = tasksMap.values().stream()
            .sorted(new DateSorter())
            .collect(Collectors.toList());
        return new TaskMap(sorted);
    }

    public TaskMap sortListByPriority() {
        // Sort by priority, same priority then date
        List<Task> sorted = tasksMap.values().stream()
            .sorted(new DateSorter())
            .sorted(new PrioritySorter())
            .collect(Collectors.toList());
        return new TaskMap(sorted);
    }

    public TaskMap searchDescription(String toSearch) {
        List<Task> found = tasksMap.values().stream()
            .filter(task -> task.getDescription().contains(toSearch))
            .collect(Collectors.toList());
        return new TaskMap(found);
    }

    public void clear() {
        tasksMap.clear();
    }

    public void delete(Integer key) {
        tasksMap.remove(key);
    }
}