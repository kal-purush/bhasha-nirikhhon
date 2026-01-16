package com.example.haarmonika;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Databaseconnection {
    //KONSANT der indeholder URL'EN TIL DATABASEN
    //JDBC URL format: jdbc:mysql://[host]:port]/[database]
    private static final String URL = "jdbc:mysql://localhost:3306/";
    // MySQL-brugernavn. Skal matche brugernavnet på din database.
    private static final String USER = "root";

    // MySQL-adgangskode. Sørg for at holde denne sikker og ikke dele den offentligt.
    private static final String PASSWORD = "Root";

    // Denne metode opretter forbindelse til databasen og returnerer et Connection-objekt.
    public static Connection getConnection() {
        try {
            // DriverManager håndterer forbindelsen til databasen.
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            // Hvis noget går galt, udskrives fejlen i konsollen.
            e.printStackTrace();
            return null; // Returner null, hvis forbindelsen mislykkes.
        }
    }
}