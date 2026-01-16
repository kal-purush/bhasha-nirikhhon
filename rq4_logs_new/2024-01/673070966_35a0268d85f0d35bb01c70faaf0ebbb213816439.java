package de.junkerjoerg12.Tetris;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class DataManger {

  private static DataManger mngr;
  private static final String FILE_SCORES = "scores.txt";

  private DataManger() {
  }

  public static DataManger getDataManger() {
    if (mngr == null) {
      mngr = new DataManger();
    }
    return mngr;
  }

  public int getGeneralHighscore() {
    try {
      BufferedReader reader = new BufferedReader(new FileReader("scores.txt"));
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains("Abs Highscore: ")) {
          reader.close();
          System.out.println("found");
          return Integer.parseInt(line.substring(line.indexOf(':') + 2));
        }
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  public static void saveData(String data) throws IOException {
    Path filePath = Paths.get(FILE_SCORES);
    Files.write(filePath, data.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  public static String loadData() throws IOException {
    Path filePath = Paths.get(FILE_SCORES);
    return new String(Files.readAllBytes(filePath));
  }

  public static void main(String[] args) {
    System.out.println(getDataManger().getGeneralHighscore());
    // try {
    // saveData("Hello, World!");
    // System.out.println("Data saved successfully.");

    // String loadedData = loadData();
    // System.out.println("Loaded data: " + loadedData);
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // String filePath = "scores.txt";

    // try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
    // String line;
    // while ((line = br.readLine()) != null) {
    // // Process each line as needed
    // System.out.println(line);
    // }
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
  }
}