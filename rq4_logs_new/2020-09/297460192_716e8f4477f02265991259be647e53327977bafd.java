package com.shoppingapp.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.shoppingapp.model.Customer;

public class UserDaoImpl implements UserDao
{

	public static Connection getConnection() 
	{
		Connection conn = null;
		
		try 
		{
			Class.forName("com.mysql.cj.jdbc.Driver");
			
			conn = DriverManager.getConnection("jdbc:mysql://localhost/shopappdb?"
					+ "autoReconnect=true&useSSL=false","root","root" );
			return conn;
		}
		catch(Exception e) 
		{
			
		}
		return conn;
		
		
	}

	
	@Override
	public Customer create(Customer cust)
	{
		if(findByUserName(cust.getUserName()) == null)
		{
			try
			{
				Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(
						"insert into customers(userName,userPass)values(?,?)");
				ps.setString(2, cust.getUserName());
				ps.setString(3, cust.getUserPass());
				ps.executeUpdate();
				con.close();
			} catch (Exception e)
			{
				System.out.println("Create Exception");
			}
			return cust;
		}
		else
		{
			return null;
		}
	}

	@Override
	public void update(Customer cust)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteByName()
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public Customer findByUserName(String userName)
	{
		Customer cust = null;
		try
		{
			Connection con = getConnection();
			PreparedStatement ps = con.prepareStatement(
					"select * from customers where userName = ?");
			ps.setString(1, userName);
			ResultSet rs = ps.executeQuery();
			if(rs.next()) 
			{
				cust = new Customer(rs.getString(2),rs.getString(3));
				con.close();
				return cust;
			}
			else
			{
				con.close();
				return null;
			}
			
		} catch (Exception e)
		{
			System.out.println("find by UserName Exception");
		}
		return null;
	}
}