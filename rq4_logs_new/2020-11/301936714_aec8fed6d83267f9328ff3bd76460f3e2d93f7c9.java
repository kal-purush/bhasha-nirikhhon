package skunk.domain;

import edu.princeton.cs.introcs.StdIn;
import edu.princeton.cs.introcs.StdOut;

public class FromFile implements SkunkInput 
{
	//**********************************************************
	
	private String strNumOfPlayers = "3";
	private String[] strNames = {"Quan", "Ayan", "Shivani"};
	private String strYes = "y";
	private String strNo = "n";
	private boolean bYes0No1 = true;
	
	//**********************************************************
	
	@Override
	public void welcomeString()
	{
		StdOut.println("***** Welcome to the Skunk game created by Seagators: FromFile  *****");
		StdOut.println("-----------------------------------------------------------");
	}
	
	//**********************************************************
	
	@Override
	public String printLineReadResponse(String szLine) 
	{
		StdOut.println( szLine );
		String strResponse =  strNumOfPlayers;
		StdOut.println( strResponse );
		return strResponse;
	}
	
	//**********************************************************
	
	@Override
	public String printLineReadNames( int iIndex, String szLine)
	{
		StdOut.println( szLine );
		
		String strResponse =  strNames[iIndex];
		StdOut.println( strResponse );
		return strResponse;
	}

	//**********************************************************
	
	public String printLineRead_Yes_No(String szLine)
	{
		StdOut.println( szLine );
		String strResponse =  bYes0No1 ? strYes: strNo;
		StdOut.println( strResponse );
		return strResponse;
	}

	//**********************************************************
	
	public boolean get_bYes0No1() 
	{
		return bYes0No1;
	}

	//**********************************************************
	
	public void set_bYes0No1( boolean bState ) 
	{
		this.bYes0No1 = bState;
	}
	
	//**********************************************************
}