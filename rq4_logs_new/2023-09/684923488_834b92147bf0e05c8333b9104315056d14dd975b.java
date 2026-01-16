package ru.aston.tarakanov_aa.task4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;


public class Main {

	public static void main(String[] args) {
		String url = "jdbc:h2:mem:testdb";
        String user = "sa";
        String password = "";

        
        try {
        	Connection connection = DriverManager.getConnection(url, user, password);
			Statement stmp = connection.createStatement();
//			stmp.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
//			stmp.execute("INSERT INTO TEST VALUES(1, 'Hello')");
//			stmp.execute("INSERT INTO TEST VALUES(2, 'World')");
			stmp.execute("RUNSCRIPT FROM 'classpath:ru/aston/tarakanov_aa/task4/data1.sql'");
			ResultSet rs = stmp.executeQuery("SELECT * FROM TEST");
			
			while(rs.next()) {
				System.out.println(rs.getString("id"));
			}
			} catch (SQLException e) {			
				e.printStackTrace();
			}
        
        

	}

}