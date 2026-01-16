package com.shoppingapp.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.shoppingapp.model.Item;

public class InventoryDaoImpl implements InventoryDao
{

	public static Connection getConnection() 
	{
		Connection conn = null;
		
		try 
		{
			Class.forName("com.mysql.cj.jdbc.Driver");
			//?"+"autoReconnect=true&useSSL=false
			conn = DriverManager.getConnection("jdbc:mysql://localhost/shopappdb","root","root" );
			
			return conn;
		}
		catch(Exception e) 
		{
			
		}
		return conn;
		
		
	}

	
	@Override
	public void create(Item item)
	{
		
	}

	@Override
	public void update(Item item)
	{
		
		
	}

	@Override
	public void deleteByItemName()
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Item> getInventory()
	{
		List<Item> inventory = new ArrayList<Item>();
		try
		{
			Connection con = getConnection();
		
			PreparedStatement ps = con.prepareStatement(
					"select * from inventory");
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) 
			{
				inventory.add(new Item(rs.getString(2),rs.getString(3),rs.getDouble(4),rs.getInt(5)));
				
			}
			return inventory;
		} catch (Exception e)
		{
			System.out.println("get inventory Exception");
		}
		return null;
	}
}