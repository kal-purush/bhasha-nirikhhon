package main.java.com.file;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

public class ParserXML implements Parser {
	
	private int startRow = 0;

	@Override
	public void parseToMemory(File sourceFile) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void parseToDB(File sourceFile, String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void parseToCSV(File sourceFile, File targetFile) {
		Set<String> attributeHeader = new HashSet<String>();
		XMLInputFactory xf;
	    XMLStreamReader xsr;	    
		int count = 0;
		try {
	    	 xf=XMLInputFactory.newInstance();
	    	 xsr = xf.createXMLStreamReader(new InputStreamReader(new FileInputStream(sourceFile)));
	    	 while (xsr.hasNext()) {		    	 			    	 
	    	 	if(count >= startRow) {		    	    
		    	    switch (xsr.getEventType()) {
		    	    case XMLStreamConstants.START_ELEMENT:		    	    	
		    		    for (int i=0; i < xsr.getAttributeCount(); i++) {				    		    			   
			    			attributeHeader.add(xsr.getAttributeLocalName(i));					    			
		    			}			    				    			 		    	  
		    	    break;
	    	 		}
	    	 	}
	    	    xsr.next();
	    	    count++;
		    }	
		    xsr.close();
		} catch (Exception e1) {	
			e1.printStackTrace();
		}
		int countOfHeaderElements = 0;
		Map<String, Integer> headerAndLocation = new HashMap<String, Integer>();		
		Iterator<String> iterator = attributeHeader.iterator();
		while(iterator.hasNext()) {
			String setElement = iterator.next();	        
			headerAndLocation.put(setElement, countOfHeaderElements);
			countOfHeaderElements++;
		}		
			
		int numberofItems = headerAndLocation.size();
		String[] headers = new String[numberofItems];
		for(Map.Entry<String, Integer> entry: headerAndLocation.entrySet()) {
			headers[entry.getValue()] = entry.getKey();
		}
		String headerString = "";
		for(String string: headers) {
			headerString = headerString +"," + string;
		}		
		
		try {
	    	xf=XMLInputFactory.newInstance();		   
			PrintWriter out = null;
	    	out = new PrintWriter(new BufferedWriter(new FileWriter(targetFile, true)));
			xsr = xf.createXMLStreamReader(new InputStreamReader(new FileInputStream(sourceFile)));			
			out.write(headerString.substring(1));
			out.write("\n");
			count =0;
			    while (xsr.hasNext()) {
			    		headers = new String[numberofItems];			    		
			    	 	if(count >= startRow) {
			    	    switch (xsr.getEventType()) {
			    	    case XMLStreamConstants.START_ELEMENT:
			    	    	String abc = "";
			    		    for (int i=0; i < xsr.getAttributeCount(); i++) {			    		    	
				    			    String value = xsr.getAttributeValue(i);				    			   
				    			    headers[headerAndLocation.get(xsr.getAttributeLocalName(i))] =  value;				    			   
			    			}
			    		    for(String string: headers) {
			    		    	 abc = abc +","+ string;
			    		    }			    		    
			    			  out.write(abc.substring(1));
			    			  out.write("\n");			    	  
			    	    break;
			    	    case XMLStreamConstants.END_ELEMENT:
			    	    break;
			    	    case XMLStreamConstants.SPACE:
			    	    break;	
			    	    case XMLStreamConstants.CHARACTERS:
			    	    break;
			    	    case XMLStreamConstants.PROCESSING_INSTRUCTION:
			    	    break;
			    	    case XMLStreamConstants.CDATA:			    	     
			    	    break;
			    	    case XMLStreamConstants.COMMENT:			    	      
			    	    break;
			    	    case XMLStreamConstants.ENTITY_REFERENCE:
			    	   	break;
			    	    case XMLStreamConstants.START_DOCUMENT:		    	      
			    	    break;
			    	    }
			    	 	}
			    	    xsr.next();
			    	    count++;
			    }	
			    xsr.close();
			    out.close();		
		}  catch (Exception e1) {
	
			e1.printStackTrace();
		}		
	}

	@Override
	public List<String> getInMemoryParse() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStartRow(int row) {
		this.startRow = row;		
	}

}

