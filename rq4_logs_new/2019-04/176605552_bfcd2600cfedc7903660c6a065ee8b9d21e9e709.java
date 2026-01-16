
package mySProject;

import java.util.ArrayList;
import java.util.Arrays;


public class DownloadTemplateMenu {
    
    public static void show() {       
        System.out.println("Download Template Page");
        System.out.print("Please enter a stream ID: ");
    }

    public static String getStreamID() {
        
        EmployeePerformanceDAO empPerfDAO = new EmployeePerformanceDAO();
        String streamID = "";
        
        do{
            streamID = App.scanner.nextLine();
        }while(!empPerfDAO.streamExists(streamID));

        return streamID;
    }
    
    // asks user for a stream ID until a correct one is inputted
    // grabs the static column names and the matching module names
    //      to place into the template excel file
    public static void createTemplate() {
        EmployeePerformanceDAO empPerfDAO = new EmployeePerformanceDAO();
        ExcelWriter excelWriter = new ExcelWriter();
        
        DownloadTemplateMenu.show();
        String streamID = DownloadTemplateMenu.getStreamID();
        
        ArrayList<String> columnTitles = new ArrayList<String>();
        columnTitles.add("Name"); // static columns
        columnTitles.add("Employee ID");
        columnTitles.add("Email");
        
        ArrayList<String> moduleNames = empPerfDAO.getModuleNamesForStream(streamID);
        
        columnTitles.addAll(moduleNames);
        
        excelWriter.createExcelFileWithTitles(columnTitles);
    }
    
}