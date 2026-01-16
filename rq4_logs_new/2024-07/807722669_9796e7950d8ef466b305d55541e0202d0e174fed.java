package service;

import model.Epic;
import model.Subtask;
import model.Task;

public class FileBackedTaskManager extends InMemoryTaskManager {

    /**
     * Добавление обычной задачи
     *
     * @param task задача
     */
    @Override
    public void addTask(Task task) {
        super.addTask(task);
    }

    /**
     * Добавление Эпика
     *
     * @param epic задача
     */
    @Override
    public void addEpic(Epic epic) {
        super.addEpic(epic);
    }

    /**
     * Добавление подзадачи
     *
     * @param subtask подзадача
     */
    @Override
    public void addSubtask(Subtask subtask) {
        super.addSubtask(subtask);
    }

    /**
     * Обновление обычной задачи
     *
     * @param newTask новая задача с верным идентификатором
     */
    @Override
    public void updateTask(Task newTask) {
        super.updateTask(newTask);
    }

    /**
     * Обновление эпика
     *
     * @param newEpic новая задача с верным идентификатором
     */
    @Override
    public void updateEpic(Epic newEpic) {
        super.updateEpic(newEpic);
    }

    /**
     * Обновление подзадачи
     *
     * @param newSubtask подзадача
     */
    @Override
    public void updateSubtask(Subtask newSubtask) {
        super.updateSubtask(newSubtask);
    }

    /**
     * Удаление обычной задачи по ID
     */
    @Override
    public void deleteTaskById(int id) {
        super.deleteTaskById(id);
    }

    /**
     * Удаление всех обычных задач
     */
    @Override
    public void deleteTasks() {
        super.deleteTasks();
    }

    /**
     * Удаление эпика по ID
     */
    @Override
    public void deleteEpicById(int id) {
        super.deleteEpicById(id);
    }

    /**
     * Удаление всех эпиков
     */
    @Override
    public void deleteEpics() {
        super.deleteEpics();
    }

    /**
     * Удаление подзадачи по ID
     *
     * @param id ID подзадачи
     */
    @Override
    public void deleteSubtaskById(int id) {
        super.deleteSubtaskById(id);
    }

    /**
     * Удаление всех подзадач
     */
    @Override
    public void deleteSubtasks() {
        super.deleteSubtasks();
    }

    /**
     * Метод сохранения состояния менеджера в файл со всеми задачами
     */
    private void save() {

    }
}