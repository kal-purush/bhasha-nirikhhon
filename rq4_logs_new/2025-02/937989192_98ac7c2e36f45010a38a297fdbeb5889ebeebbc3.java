package com.example.haarmonika.Controller;
import com.example.haarmonika.LoginSystem;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LoginScreenController { ;
    LoginSystem loginSystem = LoginSystem.getInstance();




    @FXML
    private TextField usernameField;

    @FXML
    private PasswordField passwordField;

    @FXML
    private Label statusLabel;

    // Methode af button click
    @FXML
    private void handleLogin() {
        String username = usernameField.getText();
        String password = passwordField.getText();

        // instance af controller


        // Check login
        if (loginSystem.checkLogin(username, password) == true){
            statusLabel.setText("Login Successful");
        } else {
            statusLabel.setText("Login Failed");
        }
    }


    // Method to handle login




}