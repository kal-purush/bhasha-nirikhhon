package edu.nashcc.testdb;

import java.sql.*;

public class ConnectionJDBC {

    public Connection makeConnection() 
	throws SQLException {
	try {
	    Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
	} catch (ClassNotFoundException e) {
	    throw new SQLException("Unable to load driver class");
	}
	return DriverManager.getConnection("jdbc:odbc:jdbc_book", "jdbc_guest", "guest");
    }

    public Connection makeConnection(String URL) 
	throws SQLException {
	return DriverManager.getConnection(URL);
    }

    public Connection makeConnection(String DriverName, String URL) 
	throws SQLException {
	try {
	    Class.forName(DriverName);
	} catch (ClassNotFoundException e) {
	    throw new SQLException("Unable to load driver class");
	}
	return DriverManager.getConnection(URL);
    }

    public Connection makeConnection(String URL, String username, 
				     String password) 
	throws SQLException {
	return DriverManager.getConnection(URL,username,password);
    }

    public Connection makeConnection(String DriverName, String URL, 
				     String username, String password) 
	throws SQLException {
	try {
	    Class.forName(DriverName);
	} catch (ClassNotFoundException e) {
	    throw new SQLException("Unable to load driver class");
	}
	return DriverManager.getConnection(URL,username,password);
    }

    public void closeConnection(Connection c, Statement s) 
    {
	try {
	    if (s != null) s.close();
	    if (c != null) c.close();
	} catch (SQLException sqlex) {}
    }

    public static void main(String args[]) {
	ConnectionJDBC CJ = new ConnectionJDBC();
	Connection dbConnect = null;
	Statement dbStatement = null;
	try {
	    switch (args.length) {
	    case 0 : dbConnect = CJ.makeConnection(); 
		break;
	    case 1 : dbConnect = CJ.makeConnection(args[0]); 
		break;
	    case 2 : dbConnect = CJ.makeConnection(args[0],args[1]); 
		break;
	    case 3 : dbConnect = CJ.makeConnection(args[0],args[1],args[2]); 
		break;
	    case 4 : dbConnect = CJ.makeConnection(args[0],args[1],args[2],
						   args[3]); 
	        break;
	    default : 
		System.out.println("Using the default driver");
		dbConnect = CJ.makeConnection();
	    }
	    System.out.println("Made a connection!");
	    dbStatement = dbConnect.createStatement();
	    System.out.println("Made a statement!");
	} catch (SQLException sqlex) {
	    System.out.println(sqlex.getMessage());
	}
	finally {
	    CJ.closeConnection(dbConnect,dbStatement);
	    System.out.println("Closed the connection.");
	}
    }
}