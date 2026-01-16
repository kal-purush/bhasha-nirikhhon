package database_manager;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import info.GroceryInfo;
import user.User;

public class UserDataManager extends DataManager {
	
	public  UserDataManager(String username) {
		super(username);
	}
	
	public void addUser(User u) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			ps = conn.prepareStatement("INSERT INTO Users VALUES(?,?);");
			ps.setString(1, u.uname);
			ps.setString(2, getSHA(u.password));
		    ps.execute();
		} catch (SQLException sqle) {
			System.out.println("sqle: " + sqle.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		} finally {
			try {
				if(ps != null) {
					ps.close();
				}
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException sqle) {
				System.out.println("sqle closing conn: " + sqle.getMessage());
			}
		}
	}
	
	public int checkPassword(String username,String password) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String filter = "WHERE Username LIKE '" + username + "'";
		try {
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			
			ps = conn.prepareStatement("SELECT u.Username, u.Password FROM Users u " + filter);
			rs = ps.executeQuery();
<<<<<<< HEAD
			if(rs.next()) {
				String DatabasePassword = rs.getString("Password");
				if(DatabasePassword == getSHA(password)) {
					return 1;
				}else {
					return 0;
				}else {
					return 2;
=======
			
			if(rs.next()) {
				String DatabasePassword = rs.getString("Password");
				String HashPassword =  getSHA(password);
				System.out.println(HashPassword);
				System.out.println(DatabasePassword);
			
				if(HashPassword.equals(DatabasePassword)) {
					return 1;
				}else {
					return 0;
				}
			}else {
				return 2;
>>>>>>> robin4
			}

		} catch (SQLException sqle) {
			System.out.println("sqle: " + sqle.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		} finally {
			try {
				if(ps != null) {
					ps.close();
				}
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException sqle) {
				System.out.println("sqle closing conn: " + sqle.getMessage());
			}
		}
		return 0;
	}
	
    public static String getSHA(String input) 
    { 
        try { 
            // Static getInstance method is called with hashing SHA 
            MessageDigest md = MessageDigest.getInstance("SHA-256"); 
            byte[] messageDigest = md.digest(input.getBytes()); 
            BigInteger no = new BigInteger(1, messageDigest); 
            String hashtext = no.toString(16); 
            while (hashtext.length() < 32) { 
                hashtext = "0" + hashtext; 
            } 
            return hashtext; 
        } 
        // For specifying wrong message digest algorithms 
        catch (NoSuchAlgorithmException e) { 
            System.out.println("Exception thrown"
                               + " for incorrect algorithm: " + e); 
            return null; 
        } 
    } 
	
}