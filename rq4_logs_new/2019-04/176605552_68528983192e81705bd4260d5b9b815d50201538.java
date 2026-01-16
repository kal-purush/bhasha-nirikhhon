

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class userCRUD {
	
	public static void insertUser(Statement st, String userID, String pswd, String isAdmin) {
        try {
        	System.out.println("Inserting User");
            String query = "Insert into Users VALUES('" + userID + "','" + pswd + "', '" + isAdmin + "')";
            st.executeQuery(query);

        } catch (Exception ex) {
        	System.out.println("Inserting user failed");
             System.out.println(ex);
        }
		
		
	}
	
	public static void deleteUser(Statement st, String userId) {
        try {
        	System.out.println("Deleting User");

            String query = "DELETE FROM Student_Performance.Users where user_id= '" + userId + "'";
            st.executeQuery(query);



        } catch (Exception ex) {
             System.out.println(ex);
             System.out.println("Deleting User Failed");
        }
		
		
	}
	
	public static void updateUser(Statement st,String userId, String pswd, String isAdmin) {
        try {
        	System.out.println("Updating User");

            String query = "Update Users SET password = '" + pswd + "', isAdmin = '" + isAdmin 
            		+ "' WHERE user_id = '" + userId +"'";
            st.executeQuery(query);



        } catch (Exception ex) {
        	 System.out.println("Udpating user failed");
             System.out.println(ex);
        }
		
		
	}
	
}