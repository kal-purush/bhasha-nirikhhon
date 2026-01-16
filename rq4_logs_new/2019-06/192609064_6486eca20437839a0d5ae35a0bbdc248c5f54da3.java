package com.revature.driver;


import java.sql.Connection;


import com.revature.util.ConnectionFactory;


public class Driver {

	public static Connection conn = ConnectionFactory.getConnection();
	
	public static void main(String[] args) {
		
	ConnectionFactory.getConnection();
	
	}
	
	
	
	
}
