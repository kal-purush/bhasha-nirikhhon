import manager.InMemoryTaskManager;
import manager.Managers;
import modeltask.Epic;
import modeltask.Subtask;

public class Main {

    public static void main(String[] args) {
        System.out.println("Создание задач:");
        InMemoryTaskManager taskManager = (InMemoryTaskManager) Managers.getDefault();
        taskManager.addEpic(new Epic("Купить продукты", ""));
        taskManager.addEpic(new Epic("Купить еду для собаки", ""));
        taskManager.addSubTask(new Subtask("Купить молоко", "", taskManager.getEpicId(1)));
        taskManager.addSubTask(new Subtask("Купить сыр", "", taskManager.getEpicId(1)));
        taskManager.addSubTask(new Subtask("Купить хлеб", "", taskManager.getEpicId(1)));
        taskManager.getEpicId(1);
        System.out.println(taskManager.getHistory());
        taskManager.getEpicId(2);
        System.out.println(taskManager.getHistory());
        taskManager.getEpicId(1);
        System.out.println(taskManager.getHistory());
        taskManager.getSubtaskId(5);
        System.out.println(taskManager.getHistory());
        taskManager.getSubtaskId(4);
        System.out.println(taskManager.getHistory());
        taskManager.getSubtaskId(3);
        System.out.println(taskManager.getHistory());
        taskManager.getSubtaskId(5);
        System.out.println(taskManager.getHistory());
        taskManager.removeSubtask(5);
        System.out.println(taskManager.getHistory());
        taskManager.removeEpic(1);
        System.out.println(taskManager.getHistory());
    }
}