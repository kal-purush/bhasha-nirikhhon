package chapter9.exercise07;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class MyFile {
    private static final String ALICE_FILE_URI = "http://www.gutenberg.org/files/11/11.txt";
    public static void main(String[] args) {
        Path thisClassPath = Paths.get(MyFile.class.getResource("MyFile.class").getPath());
        Path out = thisClassPath.resolveSibling("alice.txt");

        URI uri = URI.create(ALICE_FILE_URI);
        try (InputStream in = uri.toURL().openStream()) {
            Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        System.out.println("Written to "+ out);
    }
}