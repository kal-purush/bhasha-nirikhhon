
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Scanner;
import java.util.StringTokenizer;
public class AddText {
	 File yourFile = new File("YourFileHere.txt"); //adds the name of your text file in the brackets
	    int numLines; //this will store the number of lines in the file
	    String[] lines; //the lines of text that make up the file will be stored here
	    User user;
	    public AddText() {
	    	String[] lines;
	    	File yourFile = new File("User.txt");
	        numLines = getNumberLines(yourFile);
	        user = new User("","","","");
	        lines = new String[numLines];//here we set the size of the array to be the same as the number of lines in the file
	        for(int count = 0; count < numLines; count++) {
	            lines[count] = readLine(count,yourFile);//here we set each string in the array to be each new line of the file
	        }
	    }
	    public int getNumberLines(File aFile) {
	        int numLines = 0;
	        try {

	            BufferedReader input =  new BufferedReader(new FileReader(aFile));
	                try {
	                    String line = null;

	                    while (( line = input.readLine()) != null){ //ReadLine returns the contents of the next line, and returns null at the end of the file.
	                        numLines++;
	                    }
	      }
	      finally {
	        input.close();
	      }
	    }
	    catch (IOException ex){
	      ex.printStackTrace();
	    }
	        return numLines;
	    }
	    
	    public int getLinesUntilUser(File aFile, String userName) {
	        int numLines = 0;
	        try {
	            BufferedReader input =  new BufferedReader(new FileReader(aFile));
	                try {
	                    String line = null;

	                    while (( line = input.readLine()) != null && !(line.contains(userName))){ //ReadLine returns the contents of the next line, and returns null at the end of the file.
	                        numLines++;
	                    }
	      }
	      finally {
	        input.close();
	      }
	    }
	    catch (IOException ex){
	      ex.printStackTrace();
	    }
	        return numLines;
	    }
	    public String readLine(int lineNumber, File aFile) {
	        String lineText = "";
	        try {

	            BufferedReader input =  new BufferedReader(new FileReader(aFile));
	                try {
	                     for(int count = 0; count < lineNumber; count++) {
	                        input.readLine();  //ReadLine returns the contents of the next line, and returns null at the end of the file.
	                     }
	                     lineText = input.readLine();
	      }
	      finally {
	        input.close();
	      }
	    }
	    catch (IOException ex){
	      ex.printStackTrace();
	    }
	        return lineText;
	    }
	    public void addEndLine( String userName,String friendName) {
	    	File fileTemp = new File("UserTemp.txt");
	    	File file = new File("User.txt");
	    	 PrintWriter writerTemp = null;
	    	 try
	         {
	            writerTemp = new PrintWriter(new FileWriter(fileTemp,true));
	         }catch( Exception e)
	         {
	         }
	    	 


	         Scanner scan = null;
	         try{
	            scan = new Scanner( file );
	         }
	         catch( Exception e)
	         {
	         }
	         try
	         {
	            while(scan.hasNextLine())
	            {
	            	System.out.println( "he:  " + scan.nextLine() );
	               if((scan.nextLine().contains(userName)))
	               {
	            	   writerTemp.println(scan.nextLine() + user.getUserByScan( friendName).toString());
	                  
	               } 
	               else
	               {
	            	   writerTemp.println(scan.nextLine());
	               }
	           
	            }   
	         }catch(Exception e){
	            e.printStackTrace();
	         }
	         finally
	         {
	        	 scan.close();
	             writerTemp.close();
	             fileTemp = file;
	             file.delete();	
	             fileTemp.renameTo(new File("User.txt"));

	         }
	    }
	   

}