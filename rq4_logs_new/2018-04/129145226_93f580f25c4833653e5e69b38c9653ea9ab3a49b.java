package main.java.com.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CharSeperatedFile {
	private List<String> parsedRows  = null; 
    private File internalFile = null;

    public CharSeperatedFile(File file) {	   
    	setFile(file);
    }  
    
    public int getSize() {
    	int returnValue = 0;
    	if (parsedRows.size()>0) {
    		returnValue = parsedRows.size();
    	}
    	return returnValue;    	
    }
    
    public void delete() {
    	if (internalFile != null) {
	    	try{
		    	internalFile.delete();
	    	} catch(Exception e) {    		
	    		e.printStackTrace();    		
	    	}
    	}
    }
    
    public void setFile(File file) {
    	if(file.exists() && !file.isDirectory()) {
 		   this.internalFile = file;   
 		   readFile();
 	   } 
    }

    private void readFile() {
        BufferedReader   reader					= null;
        String           line                 	= null;
        parsedRows  							= new ArrayList<String>();
		try {
			reader = new BufferedReader(new FileReader (this.internalFile.getPath()));
			while((line = reader.readLine()) != null) {
                this.parsedRows.add(line);
            }
			reader.close();
		} catch (Exception e) {			
			e.printStackTrace();
		}       
    }
    
    public void addLine(String line) {
    	parsedRows.add(line);
    } 
    
    public void storeFileAt(String directory, String fileName) {
    	File tempfile = new File(directory);    	
    	if(tempfile.exists() && tempfile.isDirectory() ){    		
    		try {
                  Files.write(Paths.get(directory+"\\"+fileName), this.parsedRows, Charset.forName("UTF-8"));                     
           } catch (IOException e) {        
                  e.printStackTrace();
           }
    	}
    }

    public List<String> getParsedRows(){
           return this.parsedRows;
    }
    
    public String getRow(int row) {
    	String returnValue = "";
    	if (parsedRows.size() > row) {
    		returnValue = parsedRows.get(row);
    	}
    	return returnValue;
    }
    
    public void setParsedRows(List<String> parsedRows) {
           this.parsedRows = parsedRows;
    }

    public String getFilePath() {
           return this.internalFile.getPath();
    }

    public String getFileName() {
           return this.internalFile.getName();
    }
}