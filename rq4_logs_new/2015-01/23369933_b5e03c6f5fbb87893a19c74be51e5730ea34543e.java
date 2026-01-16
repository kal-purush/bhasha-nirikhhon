package chapter9.exercise05;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MyFile {
    public static void main(String[] args) {
        Path in = Paths.get( MyFile.class.getResource("alice.txt").getPath() );

        byte[] bytes = null;
        try {
            bytes = Files.readAllBytes(in);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        reveseByteArray(bytes);

        Path thisClassPath = Paths.get( MyFile.class.getResource(MyFile.class.getSimpleName() + ".class").getPath() );
        Path out = thisClassPath.resolveSibling( "ecila.txt" );
        try {
            Files.write(out, bytes);
        } catch(IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        System.out.println("Writed to " + out);
    }

    private static void reveseByteArray(byte[] bytes) {
        int length = bytes.length;
        int halfLength = bytes.length / 2;
        for (int i = 0; i < halfLength; i++ ) {
            byte tmp = bytes[i];
            bytes[i] = bytes[length - 1 - i];
            bytes[length - 1 - i] = tmp;
        }
    }
}