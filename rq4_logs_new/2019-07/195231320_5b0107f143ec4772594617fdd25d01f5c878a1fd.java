package Generator;

import Model.ConstCollection;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogWriter {

  synchronized private void log(String temp) {
    String tempPath = ConstCollection.PATH + "/unsorted/result.txt";
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    try(FileWriter writer = new FileWriter(tempPath, true)){
      writer.write(temp + " " + dateFormat.format(date) + "\r\n");
      writer.flush();
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }
}