package chapter9.exercise06;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class MyFile {
    public static void main(String[] args) {
        Path in = Paths.get(MyFile.class.getResource("alice.txt").getPath());

        List<String> lines = null;
        try {
            lines = Files.readAllLines(in);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        Collections.reverse(lines);

        Path thisClassPath = Paths.get(MyFile.class.getResource("MyFile.class").getPath());
        Path out = thisClassPath.resolveSibling("ecila.txt");
        try {
            Files.write(out, lines);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        System.out.println("Writed to " + out);
    }
}