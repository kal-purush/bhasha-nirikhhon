package database_manager;


import java.sql.Connection;
	import java.sql.DriverManager;
	import java.sql.PreparedStatement;
	import java.sql.ResultSet;
	import java.sql.SQLException;
	import java.sql.Statement;
	import java.util.ArrayList;
	import javax.servlet.http.HttpSession;
	import info.RestaurantInfo;
	
	public class RestaurantData  extends DataManager {
		
		//TODO
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
				ps = conn.prepareStatement("SELECT PlaceID FROM Restaurants WHERE PlaceID = '" + placeID +"'");
				rs = ps.executeQuery();
				
				if(!rs.next()) {
					//Restaurant first added
				   ps = conn.prepareStatement("INSERT INTO Restaurants (PlaceID,RestaurantName,RestaurantRating,Address,Price,"
				   		+ "DriveTimeText,DriveTimeValue,Phone,Website,InFavorites,InDoNotShow,InToExplore) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");
				   ps.setString(1, placeID); // set first variable in prepared statement
				   ps.setString(2, name);
				   ps.setLong(3, rating);
				   ps.setString(4, address); 
				   ps.setLong(5, Integer.valueOf(priceLevel));	
				   ps.setString(6, driveTimeText);
				   ps.setLong(7, driveTimeValue); 
				   ps.setString(8, phone);
				   ps.setString(9, url);				
				   if(list == 1) {
					   ps.setLong(10, 1); 
					   ps.setLong(11, 0);
					   ps.setLong(12, 0);		
				   }else if(list == 2){
					   ps.setLong(10, 0); 
					   ps.setLong(11, 1);
					   ps.setLong(12, 0);		
				   }else if(list == 3) {
					   ps.setLong(10, 0); 
					   ps.setLong(11, 0);
					   ps.setLong(12, 1);
				   }
				   
				   System.out.println("Restaurant added successful");
				   
				   ps.execute();
				   System.out.println (ps);
				}else{
					//Restaurant already in the list
					System.out.println("user already exist");
				    
					ps = conn.prepareStatement("SELECT r.InFavorites, r.InDoNotShow, r.InToExplore FROM Restaurants r "
							+ "WHERE r.PlaceID = " + placeID);
					rs = ps.executeQuery();
					int currFav = rs.getInt("r.InFavorites");
					int currNot = rs.getInt("r.InDoNotShow");
					int currExplore = rs.getInt("r.InToExplore");
					
				   if(list == 1) {
					   if(currNot == 1){
						  currNot = 0;
					   }
					   currFav = 1;
				   }else if(list == 2 ){
					   if(currFav == 1) {
						   currFav = 0;
					   }
					   currNot = 1;
				   }else if(list == 3) {
					   if(currNot == 1){
						  currNot = 0;
					   }
					   currExplore = 1;
				   }
				   
				   PreparedStatement update = conn.prepareStatement("UPDATE Restaurants SET InFavorites = ?, "
				   		+ "InDoNotShow = ?, InToExplore = ? WHERE PlaceID = " + placeID);

					update.setInt(1, currFav);
					update.setInt(2, currNot);
					update.setInt(5, currExplore);
					update.execute();  
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
		
		//TODO
		public void removeFromList(int restaurantID, int list) {
			
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				Class.forName("com.mysql.jdbc.Driver"); // get driver for database
				conn = DriverManager.getConnection(JDBC_CONNECTION);
				PreparedStatement stmt = conn.prepareStatement("DELETE FROM Restaurants WHERE PlaceID = " + restaurantID );
				rs = ps.executeQuery();	
			}catch (SQLException sqle) {
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
		
		//TODO
		public ArrayList<RestaurantInfo> loadRestaurant(){
			
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			
			ArrayList<RestaurantInfo>  restaurantList = new ArrayList<RestaurantInfo>();
			try {
				
				
				Class.forName("com.mysql.jdbc.Driver"); // get driver for database
				conn = DriverManager.getConnection(JDBC_CONNECTION);
				
				ps = conn.prepareStatement("SELECT * FROM Restaurants");
				rs = ps.executeQuery();
				

				while(rs.next()){
					
					
					RestaurantInfo new_restaurant(rs.getString("RestaurantName", re.getInt("RestaurantRating",re.getString("PlaceID"),
							rs.getString("Address"), rs.getInt("price"), rs.getString("DriveTimeText"),
							rs.getInt("DriveTimeValue"), rs.getString("Phone"), rs.getString("Website"));

					restaurantList.add(new_restaurant);
				}
			}catch (SQLException sqle) {
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
