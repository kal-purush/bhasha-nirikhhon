/* Hello Workshop Java Program */
/* 9-12-12 */

import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;


class helloWorkshop  {
    public static void main(String[] args) {
	File dir = new File(args[0]);
        Scanner scnr;
        System.out.println("Processing files in " + dir + "...");
        try { 
	   for (File file : dir.listFiles()) {
	      scnr = new Scanner(file);
              int lineNumber = 1;
	      System.out.println(file);
              while(scnr.hasNextLine()){
                 String line = scnr.nextLine();
                 System.out.println("line " + lineNumber + " :" + line);
                 lineNumber++;
              }      
   	      scnr.close();
           }
        } catch(Exception ex) {
              System.out.println("An error occurred!");    	  
        }
        System.out.println("Done!");
    }
}