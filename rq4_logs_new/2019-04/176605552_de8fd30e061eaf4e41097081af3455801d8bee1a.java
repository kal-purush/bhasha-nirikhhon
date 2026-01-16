package reportsGenerator.sProject;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * Employee Performance Database Access Object
 * Provides methods to perform CRUD operations on Users, Employees, Classes, etc.
 */
public class EmployeePerformanceDAO {
	
	private final String dbName = "Student_Performance"; // we'll always be working with this DB
	private Connection connection;
	private PreparedStatement prepStment; // prepared statement to be reused
	private CallableStatement callStment; // callable statement for procedure, function calls
	
	
	public EmployeePerformanceDAO() {
		String oracleLocalhost = "jdbc:oracle:thin:@localhost:1521:XE";
		
		try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            
            // store the connection to the DB
            connection = DriverManager.getConnection(oracleLocalhost, dbName, dbName);
            
            prepStment = null; // only set right before it's executed
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// CRUD operations can be wrapper functions around CRUD classes with static functions
	// OR
	// CRUD classes can be members of this class

	
	// STATIC FUNCTIONS EXAMPLE
	// UserCRUD class
	//		static void addUser(User newUser, PreparedStatemt st, Connection conn)
	//
	// EmployeePerformanceDAO
	//		void addUser(User user) {
	//			UserCRUD.addUser(user, prepStment, connection);
	//		}
	
	
	// CRUD CLASSES AS MEMBERS EXAMPLE
	// UserCRUD class
	//		void addUser(User newUser, PreparedStatement st, Connection conn)
	//
	// EmployeePerformanceDAO
	// 		private UserCRUD userCRUD
	//
	//		void addUser(User newUser)
	//			userCRUD.addUser(newUser, prepStment, connection)
	

}