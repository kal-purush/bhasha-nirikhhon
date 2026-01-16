package com.josh;

import java.sql.*;
import java.util.Scanner;

public class Main {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_CONNECTION_URL = "jdbc:mysql://localhost:3306/cubes";

    static final String USER = "josh";
    static final String PASSWORD = "password";

static Connection connection;
static Statement statement;

    public static void main(String[] args) throws Exception {

        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(DB_CONNECTION_URL, USER, PASSWORD);
        statement = connection.createStatement();

        statement.execute("CREATE TABLE IF NOT EXISTS cubes (cube_solver varchar(50), time_seconds float()");
        statement.execute("INSERT INTO cubes VALUES ('Cubestormer II robot', 5.270)");
        statement.execute("INSERT INTO cubes VALUES ('Fakhri Raihaan (using his feet)', 27.93)");
        statement.execute("INSERT INTO cubes VALUES ('Ruxin Liu (age 3)', 99.33)");
        statement.execute("INSERT INTO cubes VALUES ('Mats Valk (human record holder)', 6.27)");

        addTime();

        //TO DO add try/catch for all

    }

    private static void addTime() throws Exception {
        Scanner nameOfSolverScanner = new Scanner(System.in);
        System.out.println("Enter your name: ");
        String nameOfSolver = nameOfSolverScanner.next();
        Scanner timeOfSolverScanner = new Scanner(System.in);
        Float timeOfSolver = timeOfSolverScanner.nextFloat();

        String preparedStatementInsert = "INSERT INTO cubes VALUES ( ?, ? )";
        PreparedStatement psInsert = connection.prepareStatement(preparedStatementInsert);
        psInsert.setString(1, nameOfSolver);
        psInsert.setFloat(2, timeOfSolver);
        psInsert.executeUpdate();

    }
}

