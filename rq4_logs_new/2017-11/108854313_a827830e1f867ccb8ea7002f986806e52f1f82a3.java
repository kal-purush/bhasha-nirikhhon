import java.sql.*;

// Make sure you've added MariaDB's JAR to ClassPath

// Windows
// javac -cp '.;\path\to\mariadb-java-client.jar' MariaDemo.java
// java -cp '.;\path\to\mariadb-java-client.jar' Maria

// Linux
// javac -cp '.:/path/to/mariadb-java-client.jar' MariaDemo.java
// java -cp '.:/path/to/mariadb-java-client.jar' Maria

class Maria {

	public static void main(String args[]) {
		try {
			// Load MariaDB JDBC Driver
			new org.mariadb.jdbc.Driver();

			Connection conn = DriverManager.getConnection("jdbc:mariadb://127.0.0.1:3306/login", "root", "");

			if (conn != null)
				System.out.println("\nConnected to Database");

			// More of a Query Executor in my point of view
			Statement st = conn.createStatement();

			// String in which we'd enter the SQL Query
			String sql;

			System.out.println("Creating table SOMETABLE");
			sql = "CREATE TABLE SOMETABLE ("
				+ "name VARCHAR(10), "
				+ "text VARCHAR(100));";
			st.executeQuery(sql);

			System.out.println("Inserting Columns and values inside SOMETABLE");
			sql = "INSERT INTO SOMETABLE (name, text) VALUES('someone', 'hello world, i_iz_n00b');";
			st.executeQuery(sql);

			System.out.println("Querying to get all columns from table");
			sql = "SELECT * FROM SOMETABLE;";

			// ResultSet holds the result of the Query after execution
			// Use ResultSet only when you expect something in return
			ResultSet resultSet = st.executeQuery(sql);

			String someString[] = new String[2];

			// while there are more results
			while(resultSet.next()) {
				someString[0] = resultSet.getString("name");
				someString[1] = resultSet.getString("text");
			}

			System.out.println(someString[0] + " says " + someString[1]);

			// Make sure to keep the doors locked :D
                        conn.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}