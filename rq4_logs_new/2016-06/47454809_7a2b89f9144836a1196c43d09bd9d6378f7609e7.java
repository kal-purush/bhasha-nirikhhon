package database;

import java.sql.Connection;
import java.sql.DriverManager;

public class Database 
{
	private static String dbURL = "jdbc:derby://localhost:1527/db;create=true;user=APP;password=APP";
	
	// jdbc Connection
	private static Connection conn = null;
	
	static Connection createConnection(){
	    try{
	        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
	        //Get a connection
	        conn = DriverManager.getConnection(dbURL); 
	     
	    }catch (Exception except){
	        except.printStackTrace();
	    }
		return conn;
	}	
}