import java.io.File;
import java.io.FileWriter;
import java.io.*;
import java.util.Scanner;

public class LogAnalyzer {

	static ClientInfoNode listHead = null;

    public static void main(String[] args) throws Exception {
    	long start = System.currentTimeMillis();
    	ClientInfoNode currentptr = null;
	    File file = new File("logFile.txt");
	    //StringBuilder fileContents = new StringBuilder((int)file.length());
	    Scanner scanner = new Scanner(file);
	    //String finalFileContents="";
	    //String lineSeparator = System.getProperty("line.separator");
	    String logEntryIn = "";

	    String[] logEntryParts = null;

	    

	    try {
	        while(scanner.hasNextLine()) {
	            //fileContents.append(scanner.nextLine() + lineSeparator);
	            //Determine which client each line is about
	            logEntryIn = scanner.nextLine();

	            logEntryParts = logEntryIn.split("-");

	            if(searchList(logEntryParts[0])){
	            	//The client is already in the list
	            	// Add this entry to the array of entries
	            	currentptr = getClientNode(logEntryParts[0]);

	            	switch(logEntryParts[1].toLowerCase()){
	            		case "connecting to doc server":
	            			currentptr.logEvents[1] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "connected to doc server":
	            			currentptr.logEvents[2] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "requesting a file":
	            			currentptr.logEvents[3] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "received filename":
	            			currentptr.logEvents[4] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "sent filename":
	            			currentptr.logEvents[5] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "received cache address":
	            			currentptr.logEvents[6] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "connecting to cache server":
	            			currentptr.logEvents[7] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "connected to cache server":
	            			currentptr.logEvents[8] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "requesting file from cache":
	            			currentptr.logEvents[9] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "received file info":
	            			currentptr.logEvents[10] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "received file":
	            			currentptr.logEvents[11] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		case "is complete":
	            			currentptr.logEvents[12] = logEntryParts[logEntryParts.length -1];
	            			break;
	            		default:
	            			break;
	            	} // End of switch.
	            } else { //This client isn't in the list yet, add it.
	            	if(listHead == null){
	            		listHead = new ClientInfoNode();
	            		listHead.clientName = logEntryParts[0];
	            		listHead.logEvents[0] = logEntryParts[logEntryParts.length -1];
	            		listHead.filename = logEntryParts[2];
	            	} else {
	            		//Not the first item in the list, get the tail.
	            		currentptr = getTail();
	            		//Add a new node
	            		currentptr.next = new ClientInfoNode();
	            		currentptr = currentptr.next;
	            		currentptr.clientName = logEntryParts[0];
	            		currentptr.logEvents[0] = logEntryParts[logEntryParts.length -1];
	            		currentptr.filename = logEntryParts[2];
	            	}
	            }

	        } 
	        
	    } finally {
	        scanner.close();
	    }
	    
	    //Now go through the list to get duration values for each client
	    currentptr = listHead;



	    while (currentptr != null){
	    	//Get the file received time
	    	long endTime = getMillis(currentptr.logEvents[11]);
	    	//Get the file requested initially time
	    	long startTime = getMillis(currentptr.logEvents[3]);
	    	//Calculate the duration
	    	long duration = endTime - startTime;
	    	currentptr.durationFileGet = duration;
	    	//Move to the next list item
	    	currentptr=currentptr.next;
	    }

	    //Now write the data out as a comma delimited file.

	    FileWriter outToFile = null;

	    try{
	    	outToFile = new FileWriter("logAnalysis.csv");

	    	currentptr = listHead;
	    	while (currentptr != null){
	    		outToFile.write(currentptr.clientName + "," + currentptr.filename + "," + Long.toString(currentptr.durationFileGet) + "\n");
	    		currentptr = currentptr.next;
	    	}

	    }catch(IOException e){
	    	System.out.println("Write to file fail: " + e);
	    } finally {
	    	outToFile.flush();
	    	outToFile.close();
	    }

	    long end = System.currentTimeMillis();
	    System.out.println((end-start)/1000f + " seconds");
    }

    private static long getMillis(String eventTime){
    	//Go from a string with an event time
    	// hh:mm:ss:SSS, hours minutes, seconds milliseconds
    	// to a long value of milliseconds.
    	//
    	String[] timeParts = eventTime.split(":");
    	long hoursAsMinutes = Long.parseLong(timeParts[0]) * 60;
    	long totalMinutesAsSeconds = (hoursAsMinutes + Long.parseLong(timeParts[1])) * 60;
    	long totalSecondsAsMillis = (totalMinutesAsSeconds + Long.parseLong(timeParts[2])) * 1000;
    	long totalMillis = totalSecondsAsMillis + Long.parseLong(timeParts[3]);
    	return totalMillis;
    }


    private static ClientInfoNode getTail(){
    	//Return the pointer to the last node in the list.
    	// Assumes there is a list.
    	ClientInfoNode ptrToRtn = listHead;
    	while (ptrToRtn.next !=null){
    		ptrToRtn = ptrToRtn.next;
    	}
    	return ptrToRtn;
    }

    private static Boolean searchList(String searchItem){
    	//Searches the list for a particular client
    	//True if found, false if not found.
    	if(listHead == null){
    		return false; //The list is empty, nothing to search.
    	} else {
    		ClientInfoNode currentptr = listHead;
    		while(currentptr!=null){
    			if(currentptr.clientName.equals(searchItem)){
    				//Client found
    				return true;
    			}else{
    				//Move the pointer
    				currentptr = currentptr.next;
    			}
    		}
    		//List has been searched, didn't find anything.
    		return false;
    	}
    }

    private static ClientInfoNode getClientNode(String searchItem){
    	//Return a pointer to the desired node
    	ClientInfoNode ptrToRtn = listHead;
    	while(ptrToRtn != null){
    		if(ptrToRtn.clientName.equals(searchItem)){
				//Client found
				return ptrToRtn;
			}else{
				//Move the pointer
				ptrToRtn = ptrToRtn.next;
			}
    	}
    	//If this is reached, then null pointer error will occur.
    	return ptrToRtn;
    }

}


	class ClientInfoNode {
		//Information about each client
		public ClientInfoNode next;
		public String clientName = "";
		public String[] logEvents = new String[13];
		public long durationFileGet = 0;
		public String filename = "";
		public void ClientInfoNode(){

		}

	}