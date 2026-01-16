package edu.wsu.KheperaSimulator;
import java.io.*;

public class ClientConfiguration 
{
	// Configuration file path
	private static final String path = "./client.conf";
		
	// Session states
	public static final int INIT = 10;
	public static final int WAITING = 11;
	public static final int RUNNING = 12;
	public static final int TIMEOUT = 13;
	public static final int BROKEN = 14;
	public static final int STUCK = 15;
	
	// To display debugging messages in the console set to true
	public static final boolean DEBUG = false;
	
	
	// =========================================================================
	// ===												 	====================
	// ===		You can set the following in client.conf	====================
	// ===													====================
	// =========================================================================
		
	// Thread sleep times
	public long CONTROLLER_TIMEOUT;
	//public long LISTENER_TIMEOUT;
		
	// Other items...
	public String PATH;
	public String IP;
	public int	PORT;
	public String WEBCAM_URL;


	ClientConfiguration()
	{
		loadDefaults();
		
		BufferedReader in;
		String line = "#";
		
		try
		{
			in = new BufferedReader(new FileReader( path ));
		}
		
		catch (Exception e)
		{
			System.out.println("The configuration file, client.conf was not found.");
			System.out.println("Using default configuration");
			System.out.println("\nSee the Remote Client documentation to learn what");
			System.out.println("this file is used for, and how to create it.");
			return;
		}
		
		
		while( line != null )
		{
			if( line.startsWith("@CONTROLLER_TIMEOUT") )
			{
				CONTROLLER_TIMEOUT = Integer.parseInt( line.substring( line.indexOf('=') + 1 ));
			}
			
			else
			if( line.startsWith("@PATH") )
			{
				PATH = line.substring( line.indexOf('=') + 1 );
			}
			
			else
			if( line.startsWith("@PORT") )
			{
				PORT = Integer.parseInt( line.substring( line.indexOf('=') + 1 ) );
			}
			
			else
			if( line.startsWith("@IP") )
			{
				IP = line.substring( line.indexOf('=') + 1 );
			}
			
			else
			if( line.startsWith("@WEBCAM_URL") )
			{
				WEBCAM_URL = line.substring( line.indexOf('=') + 1 );
			}
		
			else
			if( line.startsWith("!") )
			{
				System.out.println( line.substring(1) );
			}
		
			try
			{
				line = in.readLine();
			}
			
			catch (Exception e)
			{
				System.out.println("Error reading from client.conf.");
				System.out.println("Using default configuration");
				return;
			}
		}
		
	}
	
	private void loadDefaults()
	{
		CONTROLLER_TIMEOUT = 20;
		//LISTENER_TIMEOUT = 5;
		
		PATH = "./controllers/";
		IP = "robotserver.cs.wright.edu";
		PORT = 2600;
		//WEBCAM_URL = "http://robotcamera.cs.wright.edu:8080";
	}
}