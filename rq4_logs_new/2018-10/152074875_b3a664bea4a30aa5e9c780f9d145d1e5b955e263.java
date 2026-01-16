package app;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Executable {

	public Executable() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String Hostname = "jdbc:mysql://localhost/Company?useSSL=false";
		String Username = "Conor";
		String Password = "password";
		MySQLHandler ms = new MySQLHandler(Hostname, Username, Password);
		String InsertStatement = "INSERT into EmployeeDetails VALUES (1, \"James Matchett\", \"Belfast\", \"PE284243G\", \"1234567890\", 100000.00);";
		addEmployeeDetails();

		ResultSet rs = ms.Query("SELECT * FROM EmployeeDetails");		
		try {
			while (rs.next()) {
				String out = String.format("%d number, %s name, %s ni", rs.getInt("employeeID"),
						rs.getString("employeeName"), rs.getString("NINumber"));
				
				System.out.println(out);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void addEmployeeDetails() throws SQLException {
		Scanner in = new Scanner(System.in);
		System.out.println("Input employee ID");
		int newID = in.nextInt();
		System.out.println("Input employee name");
		String newName = in.next();
		System.out.println("Input employee address");
		String newAddress = in.next();
		System.out.println("Input employee NIN");
		String newNIN = in.next();
		System.out.println("Input employee bank number");
		String newBankNumber = in.next();
		System.out.println("Input employee starting salary");
		float newSalary = in.nextFloat();
		String Hostname = "jdbc:mysql://localhost/Company?useSSL=false";
		String Username = "Conor";
		String Password = "password";
		MySQLHandler ms = new MySQLHandler(Hostname, Username, Password);
		ms.Statement(String.format("INSERT into EmployeeDetails VALUES (%d, %s, %s, %s, %s, %f)", newID, newName, newAddress, newNIN, newBankNumber, newSalary));
	}

}