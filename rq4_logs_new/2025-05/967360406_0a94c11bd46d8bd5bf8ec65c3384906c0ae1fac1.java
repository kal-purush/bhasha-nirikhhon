package taskManager;

import taskManager.model.Task;

import java.util.ArrayList;
import java.util.List;

public class InMemoryHistoryManager implements HistoryManager { // Реализация истории просмотров в памяти

    private static List<Task> last10ViewedTask = new ArrayList<>();

    @Override
    public void add(Task task) {
        if (last10ViewedTask.size() >= 10) {
            last10ViewedTask.remove(0);
        }
        last10ViewedTask.add(task);
    }

    @Override
    public List<Task> getHistory() {
        return new ArrayList<>(last10ViewedTask);
    }
}