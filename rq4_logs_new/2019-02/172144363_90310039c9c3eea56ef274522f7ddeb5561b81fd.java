package ir.acharkit.cli.lib;

import java.io.*;

public class FileHelper {

    private static String readFile(String filename) {
        String content = null;
        File file = new File(filename);
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            char[] chars = new char[(int) file.length()];
            reader.read(chars);
            content = new String(chars);
            reader.close();
        } catch (IOException e) {
            Logger.error(e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    Logger.error(e.getMessage());
                }
            }
        }
        return content;
    }

    private void stringToFile(String text, String fileName) {
        try {
            File file = new File(fileName);
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(text);
            bw.close();
        } catch (IOException e) {
            Logger.error(e.getMessage());
        }
    }
}