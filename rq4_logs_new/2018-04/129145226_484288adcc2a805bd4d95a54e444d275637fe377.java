package main.java.com.file;

import java.io.File;
import java.util.List;

import main.java.com.parser.Parser;
import main.java.com.parser.ParserFactory;
import main.java.com.util.StringUtil;


public class LocalFile {
	
	private List<String> parsedRows  = null; 
    private File internalFile = null;
    private Parser parser = null;
    
    public LocalFile(File file) {	   
    	this.internalFile = file; 
    	parser = ParserFactory.getParser(file.getName());
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
    
    public void loadToMemory(int startRow) {
    	parser.setStartRow(startRow);
    	if(internalFile.exists() && !internalFile.isDirectory()) { 		     
 		   parsedRows = parser.parseToStrings(internalFile);
 	   } 
    }
    
    public void addLineToMemory(String line) {
    	parsedRows.add(line);
    }
    
    public void storeAsCSV(File targetFile) {
    	parser.parseToCSV(internalFile, targetFile);
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
           return internalFile.getPath();
    }

    public String getFileName() {
           return StringUtil.getUpperCaseNameWithoutExtension(internalFile.getName());
    }
}