package gui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class WindowManager {
    private static final String SAVE_FILE_PATH = System.getProperty("user.home") + "/RobotsSaveData/save_data.json";
    private final HashMap<String, Integer> stateMap;

    public WindowManager() {
        stateMap = loadFromDisk();
    }

    private HashMap<String, Integer> loadFromDisk() {
        File file = new File(SAVE_FILE_PATH);
        if (!file.exists()) {
            return new HashMap<>();
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(file, new TypeReference<HashMap<String, Integer>>() {});
        } catch (IOException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    public void loadWindowState(MyWindow window) {
        if (window != null) {
            window.loadState(stateMap);
        }
    }

    public void saveWindowState(MyWindow window) {
        if (window != null) {
            stateMap.putAll(window.saveState());
        }
    }

    public void saveToDisk() {
        try {
            Path dir = Paths.get(System.getProperty("user.home"), "RobotsSaveData");
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(SAVE_FILE_PATH), stateMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}