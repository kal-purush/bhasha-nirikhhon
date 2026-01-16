package dk.via.chatsystem.server;

import dk.via.chatsystem.model.Message;
import dk.via.chatsystem.model.User;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;

public class Logger {
    private static Logger instance;
    private Logger() {
    }

    public void logMessage(User sender, String content, long sentAt) {
        var message = new Message(content, sender.getUsername(), sentAt);
        writeToFile(message.toDetailedString());
    }

    public static Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }

        return instance;
    }

    private void writeToFile(String message) {
        try {
            File file = new File("log.txt");
            FileWriter writer = new FileWriter(file, true);
            writer.write(message + "\n");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}