package src.ru.avdotchenkov.lesson2;

import java.sql.*;

public class Main {
	
	private static Connection connection;
	private static Statement stmt;
	private static ResultSet rs;
	
	//execute() главный метод
	//executeQuery-SELECT для возврата чего либо
	//executeUpdate - UPDATE,
	
	public static void main (String[] args) {
		try {
			connection();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		
		try {
//			findAllStudent();
//			findStudentByAge(28, 35);

//			System.out.println(getUsernameByLoginAndPassword("martin", "ffdfd"));
//			System.out.println(getUsernameByLoginAndPassword("martin", "1111"));
//			System.out.println(getUsernameByLoginAndPassword("gena", "2222"));
//			System.out.println(getUsernameByLoginAndPassword("batman", "3333"));

//			updateUsername("gena", "Гендальф белый");

//			createUsers();
			
			prepareStatment();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		try {
			disconnect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	//добовляем новый участников
	private static void createUsers () throws SQLException {
		long startTime = System.currentTimeMillis();
		
		connection.setAutoCommit(false);//добавили для нового метода
		//плохой метод, 7,5 секунд, долго.
		for (int i = 0; i < 1000 ; i++) {
			stmt.executeUpdate(String.format("INSERT INTO auth (login, password, username) VALUES ('user%s', '%s', " +
					                                 "'username%s')", i, i * 100, i));
			
		}
		stmt.executeBatch();//добавили для нового метода
		connection.setAutoCommit(true);//добавили для нового метода(сократилось до 80 милисикунд)
		System.out.println(System.currentTimeMillis() - startTime);
	}
	
	
	private static void connection () throws ClassNotFoundException, SQLException {
		Class.forName("org.sqlite.JDBC");
		connection = DriverManager.getConnection("jdbc:sqlite:mainBd/mainDB.db");
		
		stmt = connection.createStatement();
	}
	
	private static void disconnect () throws SQLException {
		connection.close();
	}
	
	//выводим студентвов
	private static void findAllStudent () throws SQLException {
		ResultSet rs = stmt.executeQuery("SELECT name, age FROM students");
		while (rs.next()) {
			System.out.printf("Name: %7s, age: %2s%n", rs.getString("name"), rs.getInt("age"));
		}
	}
	
	//Выводит через сортировку по возрасту .
	private static void findStudentByAge (int minAge, int maxAge) throws SQLException {
		rs = stmt.executeQuery(
				String.format("SELECT name, age FROM students WHERE age BETWEEN '%s' AND " + "'%s'", minAge, maxAge));
		while (rs.next()) {
			System.out.printf("Name: %7s, age: %2s%n", rs.getString("name"), rs.getInt("age"));
		}
	}
	
	//выводим username, из новой таблице
	private static String getUsernameByLoginAndPassword (String login, String password) throws SQLException {
		rs = stmt.executeQuery(String.format("SELECT * FROM auth WHERE login = '%s'", login));
		if (rs.isClosed()) {
			return null;
		}
		String username = rs.getString("username");
		String passwordDB = rs.getString("password");
		
		return ((passwordDB != null) && (passwordDB.equals(password))) ? username : null;
		
		
	}
	// меняем никнейм
	private static void updateUsername (String login, String username) throws SQLException {
	stmt.executeUpdate(String.format("UPDATE auth SET username = '%s' WHERE login = '%s'", username, login));
	}
	
	
	//еще быстрее метод по добавления 46 милисекунд
	private static void prepareStatment () throws SQLException {
		long startTime = System.currentTimeMillis();
		
		PreparedStatement pstmt =
				connection.prepareStatement("INSERT INTO auth (login, password, username) VALUES (?, ?, ?)");
		
		connection.setAutoCommit(false);
		for (int i = 0; i < 1000 ; i++) {
			pstmt.setString(1, "user" + i);
			pstmt.setString(2, "pass" + i);
			pstmt.setString(3, "username" + i);
			
			pstmt.addBatch();
		}
		
		pstmt.executeBatch();
		connection.setAutoCommit(true);
		
		System.out.println(System.currentTimeMillis() - startTime);
}



}