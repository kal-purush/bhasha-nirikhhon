package com.qts;

import com.jscape.inet.telnet.*;
import java.io.*;

public class TelnetClient extends TelnetAdapter {

  private Telnet telnet = null;
  private OutputStream output = null;
  private static BufferedReader reader = null;
  private boolean connected = false;

  public TelnetClient(String hostname) throws IOException {

    
    // create new Telnet instance
    telnet = new Telnet(hostname);
    telnet.setPort(4555);

    // register this class as TelnetListener
    telnet.addTelnetListener(this);

    try{
	    // establish Telnet connection
	    telnet.connect();
    }catch(Exception ex){
        System.out.println("exception caught while connecting" +ex);
    }
    
    connected = true;
    
  }
  
  
  private String addUser(String userName, String password) throws Exception{
	  
	  String status = "";
	  String input = null;
	  
	// get output stream
	    output = telnet.getOutputStream();

	    // sends all data entered at console to Telnet server
	    System.out.println("going to add user through telnet session");
	    String serverLogin = "ssr";
	    String serverPasswd = "ssr";
	    input = "adduser " + userName + " " + password;
	    
	    if (connected) {        
	        ((TelnetOutputStream) output).println(serverLogin);
	        //int command = telnet.TSC_CR;
	        ((TelnetOutputStream) output).println(serverPasswd);
	        //command = telnet.TSC_CR;
	        ((TelnetOutputStream) output).println(input);
	        
	        status = "User successfully added.";
	    } else {
	        //break;
	    	System.out.println("could not connect to server");
	    }
	    telnet.disconnect();
	    
	  return status;
	  
  }
  
  private String deleteUser(String userName) throws Exception{
	  
	  String status = "";
	  String input = null;
	  
	// get output stream
	    output = telnet.getOutputStream();

	    // sends all data entered at console to Telnet server
	    System.out.println("going to delete user through telnet session");
	    String serverLogin = "ssr";
	    String serverPasswd = "ssr";
	    input = "deluser " + userName;
	    
	    if (connected) {        
	        ((TelnetOutputStream) output).println(serverLogin);
	        //int command = telnet.TSC_CR;
	        ((TelnetOutputStream) output).println(serverPasswd);
	        //command = telnet.TSC_CR;
	        ((TelnetOutputStream) output).println(input);
	        
	        status = "User successfully deleted.";
	    } else {
	        //break;
	    	System.out.println("could not connect to server");
	    }
	    telnet.disconnect();
	    
	  return status;
	  
  }
  
  public static void main(String[] args){
	  
	  try{
		  TelnetClient telnetClient = new TelnetClient("localhost");
		  
		  //String addStatus = telnetClient.addUser("saikat.das", "saikat.das");
		  //System.out.println(addStatus);
		  
		 String deleteStatus = telnetClient.deleteUser("saikat.das") ;
		 System.out.println(deleteStatus);
		  
	  }catch(Exception e){
		  e.printStackTrace();
	  }
	  
  }
  
}

