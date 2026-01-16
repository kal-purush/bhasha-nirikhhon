package com;
import java.sql.*;
public class Connect
{
	
	    public static Connection con;
	    public static Connection gettingConnection()
	    {
	        try
	        {
	            Class.forName("com.mysql.jdbc.Driver");
	            con=DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqltest","root","root");
	        }
	        catch(Exception exp)
	        {
	            System.out.println("Exception Occured"+exp);
	        }
	        return con;
	    }
	}