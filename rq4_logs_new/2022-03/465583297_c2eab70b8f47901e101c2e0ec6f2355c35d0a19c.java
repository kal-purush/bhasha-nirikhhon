package Helpers;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Log {

    public static Logger logger;
    private static FileHandler fh;

    public static void createLogger(String fileName) throws IOException {
        File f = new File(fileName + ".txt");
        if(!f.exists())
        {
            f.createNewFile();
        }
        fh = new FileHandler(fileName + ".txt", true);
        LogManager.getLogManager().reset();
        logger = Logger.getLogger(fileName);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
    }


}