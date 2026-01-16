package calendar;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class FreeTimeDriver {

  public static void main(String[] args) {

    FreeTimeFinder freeTime;
    String[] fileNames = getFiles();

    if (fileNames != null) {

      freeTime = new FreeTimeFinder(fileNames);

      try {
        freeTime.calculateBusyTimes();
        freeTime.calculateFreeTimes();
        
        for (int i = 0; i < freeTime.getSize(); i++) {
          freeTime.getFreeEvent(i).makeICS("FreeTime" + i + ".ics");
        }
      }

      catch (IOException ioe) {
        System.out.println(ioe);
      }
      
      System.out.println("Calendar files noting the free time between events have been created.");
    }

    else
      System.out.println("Error!  Invalid file names!");

  }


  /**
   * Prompts the user for a list of .ics files, then validates that list.
   * 
   * @return array of .ics files
   */
  public static String[] getFiles() {

    Scanner keybd = new Scanner(System.in);
    String input, extension;
    String[] fileNames;
    File file;

    System.out.println("Please enter a list of .ics files to read from, including the extension.");
    input = keybd.nextLine();
    keybd.close();

    fileNames = input.split("\\s+");

    for (int i = 0; i < fileNames.length; i++) {

      file = new File(fileNames[i]);

      // ensure that the file exists and has a .ics extension
      if (file.exists() && !file.isDirectory()) {
        extension = fileNames[i].substring(fileNames[i].lastIndexOf(".") + 1, fileNames[i].length());

        if (!extension.equalsIgnoreCase("ics"))
          return null;
      }

      else
        return null;
    }

    return fileNames;
  }
}