package database_manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import info.Info;
import info.RecipeInfo;
import info.RestaurantInfo;

public class OrderDataManager extends DataManager {

	public OrderDataManager(String username) {
		super(username);
	}
	
	public void setOrder(Info info) {
		Class restaurantClass = RestaurantInfo.class;
		//Class recipeClass = RecipeInfo.class;
		boolean isRes = restaurantClass.isInstance(info);
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		if(isRes) {
			RestaurantInfo infoRes = (RestaurantInfo) info;
			try {
				Class.forName("com.mysql.jdbc.Driver"); // get driver for database
				conn = DriverManager.getConnection(JDBC_CONNECTION);
				ps = conn.prepareStatement("UPDATE Restaurants SET Seq = ? WHERE PlaceID LIKE ? AND User LIKE ?");
				ps.setInt(1, infoRes.order);
				ps.setString(2, infoRes.placeID);
			    ps.setString(3, username);
			    ps.executeUpdate();
			    System.out.println("Restaurant order changed.");
			    
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
		else {
			RecipeInfo infoReci = (RecipeInfo) info;
			try {
				Class.forName("com.mysql.jdbc.Driver"); // get driver for database
				conn = DriverManager.getConnection(JDBC_CONNECTION);
				ps = conn.prepareStatement("UPDATE Recipes SET Seq = ? WHERE SpoonID = ? AND User LIKE ?");
				ps.setInt(1, infoReci.order);
				ps.setInt(2, infoReci.spoonID);
			    ps.setString(3, username);
			    ps.executeUpdate();
			    System.out.println("Recipe order changed.");
			    
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
		
	}

}