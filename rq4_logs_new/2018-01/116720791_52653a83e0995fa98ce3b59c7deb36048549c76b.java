import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connect {
   
   
   //methods
   //returns a connection to your database (throws checked Exception)
   public static Connection getConnection() throws SQLException {
      Connection conn = null;
      String url = "jdbc:mysql://localhost:3306/safeeats_db?useSSL=false";
      conn = DriverManager.getConnection(url, "student", null);
      return conn;
   }
	
	public static Connection getConnection(String user, String pass) throws SQLException {
      Connection conn = null;
      String url = "jdbc:mysql://localhost:3306/safeeats_db?useSSL=false";
      conn = DriverManager.getConnection(url, user, pass);
      return conn;
   }
   
	public static void closeConnection(Connection conn) throws SQLException {
		conn.close();
	}
}