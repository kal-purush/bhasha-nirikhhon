package com.qams.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.qams.utils.ConnectionUtils;

public class LoginDAO {
	
	public boolean checkUserEmail(String email) {
		try {
			Connection con = ConnectionUtils.getConnection();
			PreparedStatement ps = con.prepareStatement("SELECT * FROM USER WHERE email = ?");
			ps.setString(1, email);
			
			ResultSet rs = ps.executeQuery();
			
			rs.next();
			if(rs.getInt(1) >0)
				return true;
		
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}