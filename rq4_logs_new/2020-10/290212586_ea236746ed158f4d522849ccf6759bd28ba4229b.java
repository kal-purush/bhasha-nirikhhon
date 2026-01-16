package com.chatbot.permit.municipal.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 *
 * @author bfetterman
 */

/**
 * This class makes use of the application.properties file and the @value annotation to create a
 * connection to the mySQL database
 */

@Component
public class DBConnection {

  @Value("${spring.datasource.url}")
  private static String host;

  @Value("${spring.datasource.username}")
  private static String userName;

  @Value("${spring.datasource.password}")
  private static String password;

  @Autowired
  protected static Connection Connect() {
    try {

      Connection conn = DriverManager.getConnection(host, userName, password);
      return conn;

    } catch (SQLException err) {

      System.out.println(err.getMessage());

    }
    return null;
  }

}