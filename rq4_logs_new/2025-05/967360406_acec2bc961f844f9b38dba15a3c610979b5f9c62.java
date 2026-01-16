package TaskManager.Model;

import java.util.ArrayList;

public class Epic extends Task{

    protected ArrayList<Integer> listSubtaskIds = new ArrayList<>(); // subtaskIds было idSubtask

    public Epic(String name, String description) { // Конструктор для создания
        super(name, description);
    }

    public Epic(String name, String description, Integer id) { // Конструктор для обновления: наименования и описания
        super(name, description, id);
    }

    public ArrayList<Integer> getListSubtaskIds() {
        return listSubtaskIds;
    }

    public void setListSubtaskIds(ArrayList<Integer> subtaskIds) {
        this.listSubtaskIds = subtaskIds;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
                ". name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", id=" + id +
                ", status=" + status + ", подзадачи: " + listSubtaskIds +
                '}' + " \n";
    }
}