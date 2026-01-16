package database_manager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.servlet.http.HttpSession;

import info.Info;
import info.RestaurantInfo;
	
public class RestaurantDataManager  extends DataManager {
	
	public void addToList(RestaurantInfo restaurant, int list) {
		
		String name = restaurant.name;
		int rating = restaurant.rating;
		String placeID = restaurant.placeID;
		String address = restaurant.address;
		String priceLevel = restaurant.priceLevel;
		String driveTimeText = restaurant.driveTimeText;
		int driveTimeValue = restaurant.driveTimeValue;
		String phone = restaurant.phone;
		String url = restaurant.url;

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
		
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			ps = conn.prepareStatement("SELECT PlaceID FROM Restaurants WHERE PlaceID LIKE '" + placeID +"'");
			rs = ps.executeQuery();
			
			if(!rs.next()) {
				//Restaurant first added
				ps.close();
				ps = conn.prepareStatement("INSERT INTO Restaurants (PlaceID,RestaurantName,RestaurantRating,Address,Price,"
			   		+ "DriveTimeText,DriveTimeValue,Phone,Website,InFavorites,InDoNotShow,InToExplore) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");
				ps.setString(1, placeID); // set first variable in prepared statement
				ps.setString(2, name);
				ps.setLong(3, rating);
				ps.setString(4, address); 
			    ps.setLong(5, priceLevel.length());	
			    ps.setString(6, driveTimeText);
			    ps.setLong(7, driveTimeValue); 
			    ps.setString(8, phone);
			    ps.setString(9, url);				
			    if(list == 1) {
			    	ps.setLong(10, 1); 
					ps.setLong(11, 0);
					ps.setLong(12, 0);		
			    } else if(list == 2) {
				   ps.setLong(10, 0); 
				   ps.setLong(11, 1);
				   ps.setLong(12, 0);		
			    } else if(list == 3) {
				   ps.setLong(10, 0); 
				   ps.setLong(11, 0);
				   ps.setLong(12, 1);
			    }
			    ps.execute();
			    System.out.println("Restaurant added to database.");
			} else {
				//Restaurant already in the list
				PreparedStatement update = null;

				if(list == 1) {
					update = conn.prepareStatement("UPDATE Restaurants SET InFavorites = ? WHERE PlaceID LIKE '" + placeID + "'");
					update.setInt(1, 1);
				} else if(list == 2) {
					update = conn.prepareStatement("UPDATE Restaurants SET InDoNotShow = ? WHERE PlaceID LIKE '" + placeID + "'");
					update.setInt(1, 1);
				} else if(list == 3) {
					update = conn.prepareStatement("UPDATE Restaurants SET InToExplore = ? WHERE PlaceID LIKE '" + placeID + "'");
					update.setInt(1, 1);
				}
				update.execute();
				System.out.println("Existed restaurant added to list.");
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
	}
	
	public void removeFromList(String restaurantID, int list) {
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);

			PreparedStatement update = null;
			if(list == 1) {
				update = conn.prepareStatement("UPDATE Restaurants SET InFavorites = ? WHERE PlaceID LIKE '" + restaurantID + "'");
				update.setInt(1, 0);
			} else if(list == 2) {
				update = conn.prepareStatement("UPDATE Restaurants SET InDoNotShow = ? WHERE PlaceID LIKE '" + restaurantID + "'");
				update.setInt(1, 0);
			} else if(list == 3) {
				update = conn.prepareStatement("UPDATE Restaurants SET InToExplore = ? WHERE PlaceID LIKE '" + restaurantID + "'");
				update.setInt(1, 0);
			}
			update.execute();  
			System.out.println("Restaurant removed from list but kept.");
		
			ps = conn.prepareStatement("SELECT r.InFavorites, r.InDoNotShow, r.InToExplore FROM Restaurants r "
					+ "WHERE r.PlaceID LIKE '" + restaurantID + "'");
			rs = ps.executeQuery();
			rs.next();
			int currFav = rs.getInt("InFavorites");
			int currNot = rs.getInt("InDoNotShow");
			int currExplore = rs.getInt("InToExplore");	
			if(currFav == 0 && currNot == 0 && currExplore == 0) {
				PreparedStatement delete = conn.prepareStatement("DELETE FROM Restaurants WHERE PlaceID LIKE '" + restaurantID + "'");
				delete.execute();
				delete.close();
				System.out.println("Restaurant removed.");
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
	}
	
	public ArrayList<Info> loadRestaurants(int list){
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		ArrayList<Info> restaurantList = new ArrayList<Info>();
		try {
			
			Class.forName("com.mysql.jdbc.Driver"); // get driver for database
			conn = DriverManager.getConnection(JDBC_CONNECTION);
			
			if(list == 1) {
				ps = conn.prepareStatement("SELECT * FROM Restaurants WHERE InFavorites = 1");
			} else if(list == 2) {
				ps = conn.prepareStatement("SELECT * FROM Restaurants WHERE InDoNotShow = 1");
			} else if(list == 3) {
				ps = conn.prepareStatement("SELECT * FROM Restaurants WHERE InToExplore = 1");
			}
		   rs = ps.executeQuery();
		   System.out.println("Loading restaurants.");

			while(rs.next()) {
				RestaurantInfo rest = new RestaurantInfo(
					rs.getString("RestaurantName"), 
					rs.getInt("RestaurantRating"),
					rs.getString("PlaceID"),
					rs.getString("Address"), 
					rs.getInt("price"), 
					rs.getString("DriveTimeText"),
					rs.getInt("DriveTimeValue"), 
					rs.getString("Phone"), 
					rs.getString("Website"));
				
				restaurantList.add(rest);
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
		return restaurantList;
	} 
	
	
}