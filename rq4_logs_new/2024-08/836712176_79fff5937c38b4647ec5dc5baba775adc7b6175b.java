package org.example.repository;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

@AllArgsConstructor
@Data
@Slf4j
public class DatabaseConnection {
    private static Properties properties;
    private static String URL, USER, PASS;
    @Getter
    private static final String CONFIG_FILE = "src/main/resources/config/config.properties"; //

    private static void loadConfigProperties() throws IOException {
        if (properties == null) {
            properties = new Properties();
            try (InputStream is = new FileInputStream(CONFIG_FILE)) {
                properties.load(is);
                System.out.println(" url: "+ url() +" user: " +  user() + " pw: " +  password());
            } catch (IOException e) {
                System.out.println("Error loading config file: " + CONFIG_FILE);
            }
        }
    }

    private static String url() {
        return URL != null ? URL : properties.getProperty("database.url");
    }

    private static String user() {
        return USER != null ? USER : properties.getProperty("database.username");
    }

   private static String password() {
        return PASS != null ? PASS : properties.getProperty("database.password");
   }

    public static Connection getConnection() throws SQLException, IOException {
        try{
            loadConfigProperties();
            Connection con = DriverManager.getConnection(url(), user(), password());
            return con;
        } catch (Exception e) {
            System.out.println("Error connecting to the database: " + e);
            log.error(e.getMessage());
            throw e;
        }
    }
    public static void setConnection(String url, String user, String password) {
        DatabaseConnection.URL = url;
        DatabaseConnection.USER = user;
        DatabaseConnection.PASS = password;
    }
}