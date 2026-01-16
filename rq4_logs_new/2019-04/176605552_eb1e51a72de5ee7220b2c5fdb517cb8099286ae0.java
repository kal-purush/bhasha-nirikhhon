package AdminPages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Scanner;

public class AdminFunction  {
	public static void main(String[] args) {

		Scanner userIn = new Scanner(System.in); // Create a Scanner object
		Scanner sc = new Scanner(System.in);
		
		int function = 1;
		while (function != 0) {
			System.out.println("Welcome Admin");
			System.out.println("Please Enter the number cooresponding to the operation you would like to complete: ");
			System.out.println("Enter 0 to Logout.");
			System.out.println("1. Make changes to modules");
			System.out.println("2. Manage users");
			System.out.println("3. Manage courses");
			// String function = userIn.nextLine(); // Read user input
			function = userIn.nextInt();
			// System.out.println(function);
			
		

			switch (function) {
			case 0:
				//getUserLoginInfo(sc);
				break;
			case 1:
				System.out.println(" Enter to MAKE CHANGES TO MODULES FUNCTIONALITY");
				System.out.println("0. To go back");
				System.out.println("1. Create a module");
				System.out.println("2. Delete a module");
				System.out.println("3. Update a module");
				int function2 = userIn.nextInt();
				switch (function2) {
				case 0:
					break;
				case 1:
					try {
			            Class.forName("oracle.jdbc.driver.OracleDriver"); 
			            Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
			            Statement st = con.createStatement();
			            
			            AdminFunctionality.createModule(st);
			            
			            con.commit();
			            st.close();
			           con.close();
			        } catch (Exception ex) {
			             System.out.println(ex);
			             System.out.println("not connected");
			        }	
					break;
				case 2:
					try {
			            Class.forName("oracle.jdbc.driver.OracleDriver"); 
			            Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
			            Statement st = con.createStatement();
			            
			            AdminFunctionality.deleteModule(st);
			            
			            con.commit();
			            st.close();
			           con.close();
			        } catch (Exception ex) {
			             System.out.println(ex);
			             System.out.println("not connected");
			        }	
					break;
				case 3:
					try {
			            Class.forName("oracle.jdbc.driver.OracleDriver"); 
			            Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
			            Statement st = con.createStatement();
			            
			            AdminFunctionality.updateModule(st);
			            
			            con.commit();
			            st.close();
			           con.close();
			        } catch (Exception ex) {
			             System.out.println(ex);
			             System.out.println("not connected");
			        }	
					break;
				}
				break;
			case 2:
				
				manageUsers manageUser = new manageUsers();
				manageUser.init();
				
				break;
			case 3:
				try {
		            Class.forName("oracle.jdbc.driver.OracleDriver"); 
		            Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
		            Statement st = con.createStatement();
		            
		            manageCourses manageCourses = new manageCourses();
		            manageCourses.userOptionsForCourses(st);
		            
		            con.commit();
		            st.close();
		           con.close();
		        } catch (Exception ex) {
		             System.out.println(ex);
		             System.out.println("not connected");
		        }	
				break;
				
			default:
				System.out.println("No such function, please choose again.");
			}
		}
		;
	}
	

}