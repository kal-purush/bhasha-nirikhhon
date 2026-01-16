package cn.edu.nottingham.notetaking.csv;

import java.io.File;
import java.io.FileWriter;

/**
 * the class is used to create csv file for each note.
 * 
 * @author WENYU DU
 *
 */

public class CsvBuilder {

    private static String CsvPath = "";
    private final static String FORMAT = ".csv";
    private static File file;

    public synchronized static void setNotePath(String path) {
	CsvPath = path+FORMAT;
	file = new File(CsvPath);
    }

    public synchronized static void writeCsv(String log) {
	if (file == null) {
	    return;
	}
	try {
	    if (!file.exists()) {
		file.createNewFile();
	    }
	    FileWriter fw = new FileWriter(file, true);
	    fw.write(log + "\r\n");

	    fw.close();
	} catch (Exception exp) {
	    exp.printStackTrace();
	}
    }
}