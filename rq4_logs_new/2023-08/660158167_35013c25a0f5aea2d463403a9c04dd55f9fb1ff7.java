package com.dragos.database;

import com.dragos.businessLogic.Account;
import com.dragos.businessLogic.Client;

import java.sql.*;


public class SignUpQuery {

    public static boolean signUp(int bankId, String customerEmail, String password,String first_name, String last_name) throws SQLException {
        Client client = null;
        Connection myConn = null;
        PreparedStatement insertAccountStmt = null;
        PreparedStatement insertClientStmt = null;
        ResultSet generatedKeys = null;

        String dbUrl = "jdbc:mysql://localhost:3306/bankapp";
        String user = "user";
        String pass = "user";

        try {
            // 1. Get a connection to the database
            myConn = DriverManager.getConnection(dbUrl, user, pass);

            // 2. Create a statement to insert a new account
            insertAccountStmt = myConn.prepareStatement("INSERT INTO account (funds) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            insertAccountStmt.setDouble(1, 0); // Assuming you want to start with 0 funds
            insertAccountStmt.executeUpdate();

            // 3. Retrieve the generated account_id
            generatedKeys = insertAccountStmt.getGeneratedKeys();
            int accountId = -1;
            if (generatedKeys.next()) {
                accountId = generatedKeys.getInt(1);
            }

            // 4. Create a statement to insert a new client
            insertClientStmt = myConn.prepareStatement("INSERT INTO client (account_id, bank_id, email, password, first_name, last_name) VALUES (?, ?, ?, ?, ?, ?)");
            insertClientStmt.setInt(1, accountId);
            insertClientStmt.setInt(2, bankId);
            insertClientStmt.setString(3, customerEmail);
            insertClientStmt.setString(4, password);
            insertClientStmt.setString(5, first_name);
            insertClientStmt.setString(6, last_name);
            insertClientStmt.executeUpdate();


        } catch (Exception exc) {
            exc.printStackTrace();
                return false;
        } finally {
            // 6. Close the resources
            if (generatedKeys != null) {
                generatedKeys.close();
            }

            if (insertAccountStmt != null) {
                insertAccountStmt.close();
            }

            if (insertClientStmt != null) {
                insertClientStmt.close();
            }

            if (myConn != null) {
                myConn.close();
            }
        }

        return true;
    }


}






