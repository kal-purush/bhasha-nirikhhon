package helpers;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Actions {

    public static String [] getStringsFromFile(String file) throws IOException {
        List<String> strings = FileUtils.readLines(new File(file));
        return strings.toArray(new String[strings.size()]);
    }

    public static String [] getStringsFromResourceFile(String fileName) throws IOException {
        String path = Actions.class.getResource("/" + fileName).getPath();
        return getStringsFromFile(path);
    }
}