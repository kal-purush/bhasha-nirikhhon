package com.smbcgroup.training.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JDBCTest {
	
	private static Connection conn;
	
	@BeforeClass
	public static void setupConnection() throws Exception {
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		conn = DriverManager.getConnection("jdbc:derby:target/testdb;create=true");
	}
	
	@Before
	public void setupTables() throws SQLException {
		try {
			conn.prepareStatement("DROP VIEW Treasury_Employees").execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		dropTable("Employees");
		dropTable("Departments");
		
		conn.prepareStatement("CREATE TABLE Departments (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, manager_name VARCHAR(64))").execute();
		conn.prepareStatement("INSERT INTO Departments(id, name, manager_name) VALUES(1, 'Treasury', 'Alex Smith')").execute();
		conn.prepareStatement("INSERT INTO Departments(id, name, manager_name) VALUES(2, 'Loans', 'Alex Smith')").execute();
		conn.prepareStatement("INSERT INTO Departments(id, name, manager_name) VALUES(3, 'JRIA', 'Beatriz Smith')").execute();
		
		conn.prepareStatement("CREATE TABLE Employees (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, wage DECIMAL(10,2), department_id INT)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1452, 'Alison Smith', 70000, 1)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1453, 'Bob Smith', 60000, 1)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1454, 'Chris Smith', 50000, 1)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1455, 'Dinesh Smith', 50000, 2)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1456, 'Emily Smith', 90000, 3)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1457, 'Felix Smith', 80000, 3)").execute();
		conn.prepareStatement("INSERT INTO Employees(id, name, wage, department_id) VALUES(1458, 'Gonzo Smith', 100000, 3)").execute();
	}

	private void dropTable(String table) {
		try {
			conn.prepareStatement("DROP TABLE " + table).execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSelect0() throws SQLException {
		ResultSet result = conn.prepareStatement("SELECT * FROM Employees WHERE id = 1452").executeQuery();
		result.next();
		assertEquals("Alison Smith", result.getString("name"));
		assertFalse(result.next());
	}
	
	@Test
	public void testSelect1() throws SQLException {
		ResultSet result = conn.prepareStatement("SELECT COUNT(*) FROM Employees WHERE department_id = 1").executeQuery();
		result.next();
		assertEquals(3, result.getInt(1));
		assertFalse(result.next());
	}
	
	@Test
	public void testSelect2() throws SQLException {
		ResultSet result = conn.prepareStatement(
				"SELECT d.id, d.name, COUNT(*) "
				+ "FROM Departments d LEFT OUTER JOIN Employees e ON d.id = e.department_id "
				+ "GROUP BY d.id, d.name").executeQuery();
	}
	
	@Test
	public void testSelect3() throws SQLException {
		ResultSet result = conn.prepareStatement(
				"SELECT d.id, d.name, COUNT(*) "
				+ "FROM Departments d LEFT OUTER JOIN Employees e ON d.id = e.department_id "
				+ "GROUP BY d.id, d.name "
				+ "HAVING COUNT(*) > 2").executeQuery();
	}
	
	@Test
	public void testSelect4() throws SQLException {
		ResultSet result = conn.prepareStatement(
				"SELECT d.id, d.name, MIN(wage), MAX(wage), AVG(wage) "
				+ "FROM Departments d LEFT OUTER JOIN Employees e ON d.id = e.department_id "
				+ "GROUP BY d.id, d.name").executeQuery();
	}
	
	@Test
	public void testCreateView() throws SQLException {
		conn.prepareStatement(
				"CREATE VIEW Treasury_Employees AS("
				+ "SELECT * FROM Employees WHERE department_id = 1)").execute();
		ResultSet result = conn.prepareStatement(
				"SELECT * FROM Treasury_Employees").executeQuery();
		result.next();
		assertEquals("Alison Smith", result.getString("name"));
		result.next();
		assertEquals("Bob Smith", result.getString("name"));
		result.next();
		assertEquals("Chris Smith", result.getString("name"));
		assertFalse(result.next());
	}

}