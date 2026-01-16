
import java.io.*;
import java.util.Scanner;
import client.*;
import common.*;
import ocsf.server.ConnectionToClient;

public class ServerConsole implements ChatIF {

 //Class variables *************************************************
  
  /**
   * The default port to connect on.
   */
  final public static int DEFAULT_PORT = 5555;
  
  //Instance variables **********************************************
  
  /**
   * The instance of the server that created this ServerConsole.
   */
  EchoServer server;
  
  /**
   * Scanner to read from the console
   */
  Scanner fromConsole; 

  
  //Constructors ****************************************************

  /**
   * Constructs an instance of the ClientConsole UI.
   *
   * @param port The port to connect on.
   */
  public ServerConsole(int port) {
    try 
    {
      server = new EchoServer(port, this);
      // Create scanner object to read from console
      fromConsole = new Scanner(System.in); 
    } 
    	catch(IOException exception) 
    {
      display("ERROR - Could not listen for clients!");
      System.exit(1);
    }
    
    
  }

  
  //Instance methods ************************************************
  
  /**
   * This method waits for input from the console.  Once it is 
   * received, it sends it to the client's message handler.
   */
  public void accept() 
  {
    try
    {

      String message;

      while (true) 
      {
        message = fromConsole.nextLine();
     
        if (message.toString().charAt(0) == '#') { // if the first character of the input is a #
        	
        	if (!message.contains(" ")) { // only one word input
        		String command = message.substring(1); // command is the second part of the input (after #)
            	
            	switch(command) {
            	
    	        	case "quit":
    	        		display("Closing program.");
    					server.close(); // kills server
    					break;
    				
    	        	case "stop":
    	        		server.stopListening(); // stop listening for new clients
    	        		break;
    	        		
    	        	case "close":
    	        		server.stopListening();
    	        		Thread[] clientThreadList = server.getClientConnections(); // array of all clients
    	        		for (int i = 0; i < clientThreadList.length; i++) {
    	        			try {
    	        				((ConnectionToClient)clientThreadList[i]).close(); // Close the client sockets of the already connected clients
    	        			}
    	        			catch(Exception e) {}
    	        		}
    	        		break;
    	        		
    	        	case "getport":
    	        		display(Integer.toString(server.getPort()));
    	        		break;
    	        		
    	        	case "start":
    	        		if (!server.isListening()) {
    	        			server.listen();
    	        		} else if (server.isListening()) {
    	        			display("Error: server is already listening for connections.");
    	        		}
    	        		break;
    					
    	        	default:
    	        		display("Not a command."); // if used # but not a valid command
    	        		break;
   
        	}
            
        }
        else if (message.contains(" ")) { // two word input
        	String command = message.split(" ")[0];
        	String para = message.split(" ")[1];
        	
        	switch (command) {
        			
        		case "#setport":
        			if (!server.isListening()) {
	        			server.setPort(Integer.parseInt(para));
	        			display("new port: " + server.getPort());
	        		} else if (server.isListening()) {
	        			display("Error: server is already running.");
	        		}
	        		break;
	        		
        		default: 
        			display("Not a valid command.");
	        		break;
        			
        	}
        		
        }
        	
        }
        else { // if the input isn't a command
        	display("SERVER MSG> " + message);
        	server.sendToAllClients("SERVER MSG> " + message);
        }
      }
    } 
    catch (Exception ex) 
    {
      System.out.println
        ("Unexpected error while reading from console!");
    }
  }

  /**
   * This method overrides the method in the ChatIF interface.  It
   * displays a message onto the screen.
   *
   * @param message The string to be displayed.
   */
  public void display(String message) 
  {
    System.out.println("> " + message);
  }

  
  //Class methods ***************************************************
  
  /**
   * This method is responsible for the creation of the Client UI.
   *
   * @param args[0] The host to connect to.
   */
  public static void main(String[] args) 
  {
	  
    int port = DEFAULT_PORT; // initialize port number to default
    
    try
    {
    	port = Integer.parseInt(args[0]); //get port from command line
    }	
    catch(Throwable t)
    {
    	System.out.println("Wrong input. Using default port.");
    }
    
    ServerConsole serverChat = new ServerConsole(port);
    serverChat.accept();  //Wait for console data
  }

}
//End of ServerConsole class