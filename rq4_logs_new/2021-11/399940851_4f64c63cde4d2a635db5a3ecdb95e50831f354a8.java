package AddDatatoJTable;

import java.io.FileInputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException;  

public class Parser {
    
    
    public static Schedule ParseInput(File filepath){ //BufferedReader infile
        if(filepath == null){
            return null;
        }
        Schedule schedule = new Schedule();
        String line = "";  
        String splitBy = ",";  

        try   
        {  
        //parsing a CSV file into BufferedReader class constructor  
        BufferedReader file = new BufferedReader(new FileReader(filepath.getAbsolutePath()));  
        while ((line = file.readLine()) != null)   //returns a Boolean value  
        {  
        String[] ScheduleLine = line.split(splitBy);    // use comma as separator

        System.out.println("Row Number=" + ScheduleLine[0] + ", Course Name=" + ScheduleLine[1] + ", First Name Last Name=" + ScheduleLine[2] + ", Preferred Class=" + ScheduleLine[3] + ", Preferred Room= " + ScheduleLine[4] + ", Faculty Availability= " + ScheduleLine[5]  + ", Maximum Enrollment= " + ScheduleLine[6]  + ", Room Name= " + ScheduleLine[7]  + ", Room Capacity= " + ScheduleLine[8]  + ", Course Name= " + ScheduleLine[9] +"]");  
        }  
        }   
        catch (IOException e)   
        {  
        e.printStackTrace();  
        }    
        
        return schedule;
    }
}