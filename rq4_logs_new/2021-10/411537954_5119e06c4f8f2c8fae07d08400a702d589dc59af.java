package cooper.log;

import java.util.logging.*;

public class CooperLogger {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static void setupLogger() {
        LogManager.getLogManager().reset();

        // set up file handler
        try {
            FileHandler fileHandler = new FileHandler("cooperLogs.log");
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.INFO);
            logger.addHandler(fileHandler);
        } catch (java.io.IOException e) {
            logger.log(Level.SEVERE, "File logger not working.");
        }
    }
}