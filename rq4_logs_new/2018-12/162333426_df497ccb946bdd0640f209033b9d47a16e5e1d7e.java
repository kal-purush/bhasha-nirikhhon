package adventofcode.common;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
    public static List<String> readInputFile(Class callingClass) throws Exception {
        List<String> lines = new ArrayList<>();

        URI uri = callingClass.getResource("input.txt").toURI();
        lines = Files.readAllLines(Paths.get(uri), StandardCharsets.UTF_8);

        return lines;
    }
}