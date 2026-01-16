package com.softpos.core.logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author nathee
 */
public class LoggerController {
    
    private static final String DATE_FORMAT = new SimpleDateFormat("ddMMyyyy", Locale.ENGLISH).format(new Date());
    private static final String FILE_NAME = "app_" + DATE_FORMAT + ".log";
    private static final String FILE_NAME_HTML = "app_" + DATE_FORMAT + ".html";
    
    private static FileHandler fh = null;
    private static final String LOG_TYPE = "html";

    public static void inital(Logger logger) {
        if (fh == null) {
            fh = LOG_TYPE.equals("html") ? initHtmlFile():initFile();
        }
        logger.addHandler(fh);
    }

    private static FileHandler initFile() {
        try {
            SimpleFormatter formatterTxt = new SimpleFormatter();
            fh = new FileHandler(FILE_NAME, true);
            fh.setFormatter(formatterTxt);
        } catch (IOException | SecurityException e) {
        }

        return fh;
    }
    
    private static FileHandler initHtmlFile() {
        try {
            Formatter formatterTxt = new MyHtmlFormatter();
            fh = new FileHandler(FILE_NAME_HTML, true);
            fh.setFormatter(formatterTxt);
        } catch (IOException | SecurityException e) {
        }

        return fh;
    }
}