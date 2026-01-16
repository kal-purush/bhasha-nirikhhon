package com.qams.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import com.qams.models.User;
import com.qams.utils.ConnectionUtils;
import com.qams.utils.DateUtils;

public class RegistrationDAO {
	
	public int createUser(User user) {
		try {
			Connection con = ConnectionUtils.getConnection();
			PreparedStatement ps=con.prepareStatement
	             ("insert into user(firstName, lastName, email, mobile, passwordHash, createDate) values(?,?,?,?,?,?)");
	
			ps.setString(1, user.getFirstName());
			ps.setString(2, user.getLastName());
			ps.setString(3, user.getEmail());
			ps.setString(4, user.getMobile());
			ps.setString(5, user.getPasswordHash());
			ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			int i =ps.executeUpdate();
			return i;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return 0;
	}

}