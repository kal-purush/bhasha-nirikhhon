package com.notable.data;

import java.util.List;

import javax.sql.DataSource;

import com.notable.business.User;

public interface UserDAO {

	// initialize datastuff
	public void setDataSource(DataSource ds);
	
	//Create User object
	public void create(String firstName, String lastName, String street, 
			String city, String state, String zip,  String email, String password);
	
	//Update User object
	public void update(Integer id, String firstName, String lastName, String street, 
			String city, String state, String zip,  String email);
	
	//get the user from a user id
	public User getUser(Integer userId);
	
	//lists out the products
	public List<User> userList();
	
	//ostensibly delete product given a name
	public void delete(String email);
	
}