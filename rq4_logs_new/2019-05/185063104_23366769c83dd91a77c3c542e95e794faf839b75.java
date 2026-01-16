package com.qams.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.qams.utils.ConnectionUtils;

public class ForgotPasswordDAO {

	
	
	public boolean checkUserExists(String emailOrMobile) {
		try {
		Connection con = ConnectionUtils.getConnection();
			PreparedStatement ps = con.prepareStatement("SELECT id, firstName, lastName, email, mobile, passwordHash FROM user "
					+ "	WHERE email = ? OR mobile = ? ");
		ps.setString(1, emailOrMobile);
		ps.setString(2, emailOrMobile);
		
		ResultSet resultSet = ps.executeQuery();
		
		boolean resultExists = resultSet.next();
		if(resultExists)
			return true;
		
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		
		return false;
	}
	
	
	public boolean updatePassword(String emailOrMobile, String passwordHash) {
		try {
			Connection con = ConnectionUtils.getConnection();
			PreparedStatement ps = con.prepareStatement(" update user set passwordHash = ? where email = ? OR mobile = ? ");
			
			ps.setString(1, passwordHash);
			ps.setString(2, emailOrMobile);
			ps.setString(3, emailOrMobile);	
			int rowCount = ps.executeUpdate();
			if(rowCount > 0) {
				return true;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	
	
	
}