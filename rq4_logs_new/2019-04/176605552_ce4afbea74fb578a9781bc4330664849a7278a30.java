
import java.sql.*;


public class InsertFunctions {
/*Java class to access and insert new data into the DataBase
 * for the modules, categories, courses, and streams tables.
 */
	public static void main(String[] args){
	Statement state;
	Connection c1;
	ResultSet r1;
	
	try {
		//Connect to DB
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		
		
		//INSERT FOR STREAM
		
		state = c1.createStatement();
		r1 =state.executeQuery("INSERT INTO stream VALUES('TEST_ID2', 'Test Stream2')");
		System.out.println("Insert Statement Executed"); //Take this line out if you don't want a confirmation message
		
		//INSERT FOR CLASS
		Statement state2 = c1.createStatement();
		r1=state2.executeQuery("INSERT INTO class VALUES('TESTID', 'TEST_ID2', 'IN5')");
		System.out.println("Insert Statement Executed"); //Take this line out if you don't want a confirmation message
		
		//INSERT FOR MODULES
		state = c1.createStatement();
		r1=state.executeQuery("INSERT INTO modules(module_id, module_name, category, stream_id) VALUES(001, 'TEST NAME', 'TEST CATEGORY', 'DB343')");
		System.out.println("Insert Statement Executed"); //Take this line out if you don't want a confirmation message
		
		//INSERT FOR COURSES
		state=c1.createStatement();
		r1=state.executeQuery("INSERT INTO courses VALUES('A1B2', 'TEST COURSE', 10)");
		System.out.println("Insert Statement Executed"); //Take this line out if you don't want a confirmation message
	} 
	
	
	catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}