package org.uth.osdbhandle.test;

/**
 * An x row thrash test using JDBC wrapper (for OpenShift demos).
 * 
 * This class creates a database, creates a tables, populates the tables, performs 
 * some intensive queries, drops the table, drops the database.
 * 
 * @author Ian Lawson
 */
public class ThrashTest 
{
  public static void main( String[] args )
  {
    if( args.length != 5 )
    {
      System.out.println( "Usage: java ThrashTest connectionString username password, databaseToCreate, numberOfItems" );
      System.exit(0);
    }
    
    try
    {
      Integer.parseInt(args[4]);
    }
    catch( NumberFormatException exc )
    {
      System.out.println( "Incorrect number format for numberOfItems");
      System.exit(0);
    }
    
    new ThrashTest( args[0], args[1], args[2], args[3], Integer.parseInt(args[4]));
  }
  
  public ThrashTest( String conenctionString, String username, String password, String databaseName, int rows )
  {
    // This test creates a handle, creates the database, closes the handle, opens a handle to the db, does the test,
    // closes the handle, opens a handle to root and drops the database
    
  }
}