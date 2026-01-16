/********************************************************************
 *Purpose : Java Database connectivity using 7 steps
 *        1.Import java.sql*   2.Load and Register driver
 *        3. Create connection 4. Create Statement
 *        5. Execute Query     6. Process The Results
 *        7. close connection.
 * @Auther : Ganesh Gavhad
 * @since  : 15-08-2021
 * @version : 1.0
 **********************************************************************/
package com.bridgelabz;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class DemoJDBC {
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3307/payroll_service?useSSL=false";
        String userName = "root";
        String password = "Ganesh@7779";
        Connection connection;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Driver Loaded.");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot find the driver in the classpath!", e);
        }

        listDrivers();

        // Establishing connection With Database.
        try {
            System.out.println("Connecting to database: " + jdbcURL);
            connection = DriverManager.getConnection(jdbcURL, userName, password);
            System.out.println("Connection is successful! " + connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Purpose : this method is for Check all the drivers registered with the jdbc driver
     */
    private static void listDrivers() {
        Enumeration<Driver> driverList = DriverManager.getDrivers();
        while (driverList.hasMoreElements()) {
            Driver driverClass = driverList.nextElement();
            System.out.println(driverClass.getClass().getName());
        }
    }
}