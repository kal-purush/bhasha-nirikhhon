package src.spike;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ConfigReader {

    public static void main(String[] args) {

        final String FILENAME = "config.txt";

        BufferedReader br = null;
        FileReader fr = null;
        Scanner scanner;

        ArrayList<String> arealist = new ArrayList<>();
        ArrayList<ArrayList<String>> courseList = new ArrayList<>();

        try {
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                if (!sCurrentLine.isEmpty()) {
                    scanner = new Scanner(sCurrentLine);
                    if (!scanner.hasNext("0-9")){
                        arealist.add(scanner.next());
                    }
                    System.out.println(arealist);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }
}