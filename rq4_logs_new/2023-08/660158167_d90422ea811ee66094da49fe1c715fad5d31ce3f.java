package com.dragos.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ContinuousDatabaseConnection {

    public static void main(String[] args) {
        // Run the database connection until the user presses a button (e.g., enters "q")
        try {
            while (true) {
                // Check if the user pressed the exit button (e.g., entered "q")
                if (userWantsToExit()) {
                    break; // Break the loop if the user wants to exit
                }

                // Connect to the database and perform your database operations
                Connection connection = getConnection();
                // Perform your database operations here...
                // For example: execute queries, insert data, etc.

                // Close the database connection
                if (connection != null) {
                    connection.close();
                }

                // Wait for a short duration before the next database connection attempt
                Thread.sleep(1000); // You can adjust the sleep duration as needed
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to check if the user wants to exit (for demonstration purposes)
    private static boolean userWantsToExit() {
        // Implement your logic to detect user input (e.g., pressing a button)
        // For demonstration purposes, let's assume the user enters "q" to exit.
        String userInput = ""; // Get the user input here (e.g., using Scanner)
        return userInput.equalsIgnoreCase("q");
    }

    // Method to get a database connection
    private static Connection getConnection() throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/bankapp";
        String user = "user";
        String pass = "user";
        return DriverManager.getConnection(dbUrl, user, pass);
    }
}