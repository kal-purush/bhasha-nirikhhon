package com.sg.databaseconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class PreparedStatementDemo1 
{

	public static void main(String[] args) 
	{
		getRecordsFromDatabase();
		insertRecords();
	}

	private static void getRecordsFromDatabase()
	{
		Connection conn=null;
		try
		{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn=DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "System", "tiger");
			System.out.println("Connection has successful!!!");

			String strQuery="select * from dept2";
			PreparedStatement stmt=conn.prepareStatement(strQuery);
			ResultSet rs=stmt.executeQuery();
			ResultSetMetaData rsmeta=rs.getMetaData();
			String colname1=rsmeta.getColumnName(1);
			String colname2=rsmeta.getColumnName(2);
			String colname3=rsmeta.getColumnName(3);
			System.out.printf("%-12s",colname1);
			System.out.printf("%-12s",colname2);
			System.out.printf("%-12s",colname3);
			System.out.printf("\n");
			while(rs.next())
			{
				String deptnumber=rs.getString("DEPTNO");
				String deptname=rs.getString("DNAME");
				String location=rs.getString("LOC");
				System.out.printf("%-12s",deptnumber);
				System.out.printf("%-12s",deptname);
				System.out.printf("%-12s",location);
				System.out.printf("\n");
			}
		}catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				conn.close();
			}catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}

	private static void insertRecords()
	{
		Connection conn=null;
		try
		{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn=DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "System", "tiger");

			String strQuery="insert into dept2 values(80,'INSURANCE','BANGALORE')";
			PreparedStatement stmt=conn.prepareStatement(strQuery);
			stmt.executeUpdate();
			System.out.println("Inserted the records successfully!!!");
		}catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				conn.close();
			}catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}


}

