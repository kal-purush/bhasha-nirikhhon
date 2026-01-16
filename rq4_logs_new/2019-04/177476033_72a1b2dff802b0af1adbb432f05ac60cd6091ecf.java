package database_manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import info.Info;
import info.RestaurantInfo;
import info.History;

public class HistoryDataManager  extends DataManager {

	
	public void addToList(History history) {
		
		 String query = history.query;
		 int number = history.number;
		 int radius = history.radius;
		 
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
		
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			ps = conn.prepareStatement("INSERT INTO History VALUES (?,?,?);");

			ps.setString(1, query); // set first variable in prepared statement
			ps.setLong(2, number);
		    ps.setLong(3, radius);	
		    ps.execute();
		    System.out.println("Restaurant added to database.");
		    
		} catch (SQLException sqle) {
			System.out.println("sqle: " + sqle.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
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
	
	public void removeHistory(int key) {
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
				PreparedStatement delete = conn.prepareStatement("DELETE FROM History WHERE Key LIKE '" + key + "'");
				delete.execute();
				delete.close();
				System.out.println("Restaurant removed.");
		} catch (SQLException sqle) {
			System.out.println("sqle: " + sqle.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
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
	
	public ArrayList<History> loadHistory(){
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		ArrayList<History> historyList = new ArrayList<History>();
		try {
			
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			

			ps = conn.prepareStatement("SELECT * FROM History");
			rs = ps.executeQuery();
			while(rs.next()) {
				History his = new History(
					rs.getInt("Key"),
					rs.getString("Query"), 
					rs.getInt("Number"),
					rs.getInt("Rqdius"));
				historyList.add(his);
			}
		} catch (SQLException sqle) {
			System.out.println("sqle: " + sqle.getMessage());
		} catch (ClassNotFoundException cnfe) {
			System.out.println("cnfe: " + cnfe.getMessage());
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
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
		return historyList;
	} 
	
}