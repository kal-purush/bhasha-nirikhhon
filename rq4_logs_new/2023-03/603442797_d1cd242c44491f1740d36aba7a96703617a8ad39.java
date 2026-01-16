package homework.day10;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Folders {
    public static void main(String[] args) throws IOException {
        File folder = new File("d:/4/3/2/1/");
        File file1 = new File("d:/4/3/2/1/file1.txt");
        File file2 = new File("d:/4/3/2/1/file2.txt");
        Random random = new Random();

        folder.mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        BufferedWriter out1 = new BufferedWriter(new FileWriter(file1));
        BufferedWriter out2 = new BufferedWriter(new FileWriter(file2));
        for (int i = 0; i < 10; i++) {
            out1.write(" " + random.nextInt(100));
            out2.write(" " + random.nextInt(50));
        }
        out1.close();
        out2.close();
    }
}