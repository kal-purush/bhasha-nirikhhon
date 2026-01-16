package neopetspace.ui;

//CS 421
//Group 11
//Project Part 3
//Java UI

//Main interface

import java.sql.*;

public class DatabaseUI {

	static boolean _quit;
	static Printer _printer;
	static int _response;
	static String _username;
	static String _password;
	static String _url;
	static Connection connection;

	public static void main(String[] args) {
		// Tries to establish connection
		try {
			initilize();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// prints greeting
		_printer.printStart();
		// prints options
		_printer.printOptions();

		while (_response != 7) {
			_response = _printer.takeOptionInput();

			System.out.println();

			performAction();
		}
	}

	// Initilize the class variables
	static void initilize() throws SQLException {
		_quit = false;
		_printer = Printer.getInstance();
		_response = -1;
		_username = "cs421g11";
		_password = "frymvomwyd";
		_url = "jdbc:postgresql://db2.cs.mcgill.ca/CS421";

		connection = DriverManager.getConnection(_url, _username, _password);
		// connection.close();
	}

	static void performAction() {
		switch (_response) {
		case 1:
			try {
				optionOne();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		case 2:
			optionTwo();
			break;
		case 3:
			optionThree();
			break;
		case 4:
			optionFour();
			break;
		case 5:
			optionFive();
			break;
		case 6:
			optionSix();
			break;
		case 7:
			try {
				optionSeven();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		default:
			break;
		}
	}

	static void optionOne() throws SQLException {
		Statement stmt = null;
		String query = "SELECT * FROM pet_pages";
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int page_id = rs.getInt("page_id");
				String species = rs.getString("species");
				String gender = rs.getString("gender");
				int age = rs.getInt("age");
				Date birthday = rs.getDate("birthday");
				System.out.println(page_id + "\t" + species + "\t" + gender
						+ "\t" + age + "\t" + birthday);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	static void optionTwo() {
	}

	static void optionThree() {
	}

	static void optionFour() {
	}

	static void optionFive() {
	}

	// re-prints options for the user
	static void optionSix() {
		_printer.printOptions();
	}

	static void optionSeven() throws SQLException {
		connection.close();
	}

}