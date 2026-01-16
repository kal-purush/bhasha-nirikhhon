package seedu.duke;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Scanner;

public class Storage {
    private static final String DIRECTORY_NAME = "data";
    private static final String FILE_NAME = "data.json";
    private final Gson gson = new Gson();

    /**
     * Write to file the latest data of tasks.
     *
     * @param tasks latest TaskList object after modification.
     */
    public void writeTasksToFile(ArrayList<Task> tasks) {
        try (FileWriter file = new FileWriter(DIRECTORY_NAME + "/" + FILE_NAME)) {
            for (Task t : tasks) {
                file.write(gson.toJson(t) + System.lineSeparator());
            }
        } catch (IOException e) {
            System.out.println("IO EXCEPTION");
        }
    }

    /**
     * Load data from file and add tasks to TaskList.
     */
    public void loadTasks(ArrayList<Task> tasks) {
        if (!createDirectory()) {
            try {
                readTasksFromFile(tasks);
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
            }
        }
    }

    /**
     * Create a directory if the "data" directory does not exists.
     *
     * @return true if directory is at the point of execution.
     */
    private boolean createDirectory() {
        File directory = new File(DIRECTORY_NAME);
        boolean directoryCreated = false;
        boolean fileCreated = false;
        try {
            if (!directory.exists()) {
                directoryCreated = directory.mkdir();
                fileCreated = new File(DIRECTORY_NAME + "/" + FILE_NAME).createNewFile();
            }
        } catch (Exception e) {
            e.getStackTrace();
        }
        return directoryCreated && fileCreated;
    }

    /**
     * Read lines from file and process each line.
     */
    private void readTasksFromFile(ArrayList<Task> tasks) throws FileNotFoundException {
        File file = new File(DIRECTORY_NAME + "/" + FILE_NAME);
        Scanner scanner = new Scanner(file);
        Type type = new TypeToken<Task>(){}.getType();
        while (scanner.hasNext()) {
            tasks.add(gson.fromJson(scanner.nextLine(), type));
        }
    }
}