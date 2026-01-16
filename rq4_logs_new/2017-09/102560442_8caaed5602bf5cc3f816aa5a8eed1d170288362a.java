package io.c12.bala.datacopy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class Database {

	private String jdbcUrl;
	private String jdbcDriverClassName;
	private String username;
	private String password;
	private Connection connection;

	public Connection openConnection() {
		validate();
		try {
			Class.forName(jdbcDriverClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			connection = DriverManager.getConnection(jdbcUrl, username, password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}

	public void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void validate() {
		if (jdbcDriverClassName == null) {
			throw new IllegalArgumentException("jdbcDriverClassName cannot be null");
		}
		if (jdbcUrl == null) {
			throw new IllegalArgumentException("jdbcUrl cannot be null");
		}
		if (username == null) {
			throw new IllegalArgumentException("username cannot be null");
		}
		if (password == null) {
			throw new IllegalArgumentException("password cannot be null");
		}
	}

}