package service;

import Exceptions.ManagerSaveException;
import model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static model.Status.IN_PROGRESS;
import static model.Status.NEW;
import static model.Type.*;

public class FileBackedTaskManager extends InMemoryTaskManager {
    private final File file;

    public FileBackedTaskManager(File file) {
        this.file = file;
        if (!file.isFile()) {
            try {
                Path path = Files.createFile(Paths.get("Autosave", "autosave.csv"));
            } catch (IOException exception) {
                throw new ManagerSaveException("Возникла проблема при создании файла. Возможно, он уже существует");
            }
        }
    }

    @Override
    public Task create(Task task) {
        super.create(task);
        save();
        return task;
    }

    @Override
    public Epic createEpic(Epic epic) {
        super.createEpic(epic);
        save();
        return epic;
    }

    @Override
    public SubTask createSubTask(SubTask subTask) {
        super.createSubTask(subTask);
        save();
        return subTask;
    }

    @Override
    public Task getTask(int id) {
        Task task = super.getTask(id);
        if (task != null) {
            save();
            return task;
        } else {
            return null;
        }
    }

    @Override
    public Epic getEpic(int id) {
        Epic epic = super.getEpic(id);
        if (epic != null) {
            save();
            return epic;
        } else {
            return null;
        }
    }

    @Override
    public SubTask getSubTask(int id) {
        SubTask subTask = super.getSubTask(id);
        if (subTask != null) {
            save();
            return subTask;
        } else {
            return null;
        }
    }

    @Override
    public void update(Task task) {
        super.update(task);
        save();
    }

    @Override
    public void updateEpic(Epic epic) {
        super.updateEpic(epic);
        save();
    }

    @Override
    public void updateSubTask(SubTask subTask) {
        super.updateSubTask(subTask);
        save();
    }

    @Override
    public void delete(int id) {
        super.delete(id);
        save();
    }

    @Override
    public void deleteEpic(int id) {
        super.deleteEpic(id);
        save();
    }

    @Override
    public void deleteSubTask(int id) {
        super.deleteSubTask(id);
        save();
    }

    void save() {
        try (Writer writer = new FileWriter(file)) {
            writer.write("id,type,name,status,description,epic\n");
            Map<Integer, String> allTasks = new HashMap<>();

            List<Task> tasks = super.getAllTasks();
            for (Task task : tasks) {
                allTasks.put(task.getId(), task.toStringForFile());
            }

            List<Epic> epics = super.getAllEpics();
            for (Epic epic : epics) {
                allTasks.put(epic.getId(), epic.toStringForFile());
            }

            List<SubTask> subTasks = super.getAllSubTasks();
            for (SubTask subTask : subTasks) {
                allTasks.put(subTask.getId(), subTask.toStringForFile());
            }

            for (String value : allTasks.values()) {
                writer.write(String.format("%s\n", value));
            }
        } catch (IOException exception) {
            throw new ManagerSaveException("Невозможно записать в файл");
        }
    }

    private static Task fromString(String text) {
        Task task = new Task();
        int epicId = 0;
        String[] elements = text.split(",");
        int id = Integer.parseInt(elements[0]);
        Type type = valueOf(elements[1]);
        String name = String.valueOf(elements[2]);
        Status status = Status.valueOf(elements[3]);
        String description = elements[4];

        if (elements.length == 6) {
            epicId = Integer.parseInt(elements[5]);
        }

        if (type == TASK) {
            return new Task(id, name, status, description);
        } else if (type == SUBTASK) {
            return new SubTask(id, name, status, description, epicId);
        } else if (type == EPIC) {
            return new Epic(id, name, status, description);
        }
        return task;
    }

    public static FileBackedTaskManager loadFromFile(File file) {
        FileBackedTaskManager fileBackedTaskManager = new FileBackedTaskManager(file);
        String data;
        try {
            data = Files.readString(Path.of(file.getAbsolutePath()));
        } catch (IOException exception) {
            throw new ManagerSaveException("Невозможно прочитать файл");
        }
        String[] lines = data.split("\n");
        boolean isName = true;
        boolean isTask = true;
        int maxId = 0;
        int id;

        for (String line : lines) {
            if (isName) {
                isName = false;
                continue;
            }
            if (line.isEmpty() || line.equals("\r")) {
                isTask = false;
                continue;
            }
            if (isTask) {
                Type type = valueOf(line.split(",")[1]);
                switch (type) {
                    case EPIC:
                        Epic epic = (Epic) fromString(line);
                        id = epic.getId();
                        if (id > maxId) {
                            maxId = id;
                        }
                        fileBackedTaskManager.epics.put(id, epic);
                        break;

                    case SUBTASK:
                        SubTask subTask = (SubTask) fromString(line);
                        id = subTask.getId();
                        if (id > maxId) {
                            maxId = id;
                        }
                        fileBackedTaskManager.subTasks.put(id, subTask);
                        break;

                    case TASK:
                        Task task = fromString(line);

                        id = task.getId();
                        if (id > maxId) {
                            maxId = id;
                        }
                        fileBackedTaskManager.tasks.put(id, task);
                        break;
                }
            }
        }
        fileBackedTaskManager.seq = maxId;
        return fileBackedTaskManager;
    }

    public static void main(String[] args) {
        FileBackedTaskManager taskManager = new FileBackedTaskManager(new File("Autosave","autosave.csv"));
        Task task1 = taskManager.create(new Task("задача 111", NEW, "описание задачи 111"));
        Task task2 = taskManager.create(new Task("задача 222", IN_PROGRESS, "описание задачи 222"));
        Epic epic1 = taskManager.createEpic(new Epic("эпик 111 с 1 подзадачей", NEW, "описание " +
                "эпика 111"));
        Epic epic2 = taskManager.createEpic(new Epic("эпик 222 с 2 подзадачами", NEW, "описание " +
                "эпика 222"));
        SubTask subTask1 = taskManager.createSubTask(new SubTask(epic1, "подзадача 111", NEW,
                "описание подзадачи 111"));
        SubTask subTask2 = taskManager.createSubTask(new SubTask(epic2, "подзадача 222", NEW,
                "описание подзадачи 222"));
        SubTask subTask3 = taskManager.createSubTask(new SubTask(epic2, "подзадача 333", NEW,
                "описание подзадачи 333"));

        System.out.println("Оставшиеся задачи:" + taskManager.getAllTasks());
        System.out.println("Оставшиеся эпики:" + taskManager.getAllEpics());
        System.out.println("Оставшиеся подзадачи:" + taskManager.getAllSubTasks());
    }
}