package com.example.haarmonika;

import com.example.haarmonika.Model.Person;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

public class LoginScreenController {

    // We use a separate singleton class to verify login attempts, this way I/O from our database containing sensitive user data is kept separate from other operations
    LoginSystem loginSystem = LoginSystem.getInstance();
    // DatabaseRepo

    private Parent root;
    private Stage stage;
    private Scene scene;

    LoggedInUser loggedInUser = LoggedInUser.getInstance();
    Person person;


    @FXML
    private PasswordField passwordField;

    @FXML
    private TextField usernameField;

    @FXML
    private Label loginScreenLabel;

    @FXML
    protected void onLoginButtonClick() {
        try {

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        loginScreenLabel.setText("Trying to login...");
    }




    // Methode af button click
    /*
    @FXML
    private void handleLogin() {
        String username = usernameField.getText();
        String password = passwordField.getText();

        // instance af controller


        // Check login
        if (loginSystem.checkLogin(username, password) == true) {
            loginScreenLabel.setText("Login Successful");
        } else {
            loginScreenLabel.setText("Login Failed");
        }
    }


    // Method to handle login


    @FXML
    public void switchToBookingScreen(ActionEvent event) throws IOException {
        root = FXMLLoader.load(getClass().getResource("BookingScreen.fxml"));
        stage = (Stage) ((Node) event.getSource()).getScene().getWindow();
        scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    */

    // When the LoginSystem has confirmed that a login is successful, we then store the users data
    /*
    public void loginCurrentUser(LoggedInUser loggedInUser, Person person) throws IOException {
        person = databaseRepo.getUser;
        loggedInUser.setCurrentUser(person);
    }
    */


}