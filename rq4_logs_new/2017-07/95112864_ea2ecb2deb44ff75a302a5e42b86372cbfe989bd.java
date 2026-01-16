package com.musala.sdcs.db;

import java.sql.DriverManager;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;

/**
 * This class makes connection with the DB
 *
 */
public class DbUtil {
	private static final String DB_CONNECTION_URL = "connectionUrl";
	private static final String DB_PASSWORD = "password";
	private static final String DB_USERNAME = "username";
	private static final String DB_DRIVER = "dbDriver";
	private static final String DB_PROPERTIES_FILE_PATH = "db_resources/db.properties";

	private static Connection dbConnection = null;

	/**
	 * @return DB Connection
	 */
	public static Connection getConnection() {
		if (dbConnection != null) {
			return dbConnection;
		}

		try {
			File dbProperties = new File(DB_PROPERTIES_FILE_PATH);
			InputStream inputStream = new FileInputStream(dbProperties);
			Properties properties = new Properties();

			properties.load(inputStream);

			String dbDriver = properties.getProperty(DB_DRIVER);
			String username = properties.getProperty(DB_USERNAME);
			String password = properties.getProperty(DB_PASSWORD);
			String connectionUrl = properties.getProperty(DB_CONNECTION_URL);

			Class.forName(dbDriver).newInstance();
			dbConnection = DriverManager.getConnection(connectionUrl, username, password);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return dbConnection;
	}

}