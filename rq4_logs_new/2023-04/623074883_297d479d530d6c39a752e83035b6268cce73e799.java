package com.groupthree.mancala;

import com.groupthree.mancala.exceptions.UserExistsException;
import com.groupthree.mancala.models.Admin;
import com.groupthree.mancala.models.Player;
import com.groupthree.mancala.repository.UserRepository;
import javafx.collections.FXCollections;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.Text;
import javafx.scene.text.TextAlignment;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RegisterController {
    @FXML
    private VBox base;
    private TextField usernameField;
    private TextField firstnameField;
    private TextField lastnameField;
    private ComboBox<String> comboBox;
    private String userProfileImagePath = "com/groupthree/mancala/pholder.jpg";

    private Stage stage;
    private Scene scene;
    private Parent root;

    public void initializeScreen(Stage stage) throws IOException {
        // Create Components
        Text userLabel = new Text("Username");
        usernameField = new TextField("");

        Text firstnameLabel = new Text("First Name");
        firstnameField = new TextField("");

        Text lastnameLabel = new Text("Last Name");
        lastnameField = new TextField("");

        comboBox = new ComboBox<>(FXCollections.observableArrayList("Register as Admin", "Register as User"));
        comboBox.setPromptText("Select Registration Type");

        FileChooser fileChooser = new FileChooser();
        Button btnFileChooser = new Button("Select File");
        Label fileNameLabel = new Label("Select Profile Image");
        HBox hBox = new HBox(fileNameLabel, btnFileChooser);
        Hyperlink hyperlink = new Hyperlink("Sign-in here");
        Text signInText = new Text("Already have an account?");
        HBox linkToLogin = new HBox(signInText, hyperlink);
        linkToLogin.setAlignment(Pos.CENTER);

        Button registerButton = new Button("Register");

        // Styling
        registerButton.setMinWidth(200);
        registerButton.setStyle("-fx-alignment: center;");
        signInText.setTextAlignment(TextAlignment.CENTER);
        btnFileChooser.setMinWidth(50);
        comboBox.setMinWidth(200);
        GridPane.setMargin(comboBox, new Insets(10, 0, 10, 0));
        HBox.setMargin(fileNameLabel, new Insets(0, 10, 0, 0));
        hBox.setMaxWidth(200);
        hBox.setAlignment(Pos.CENTER);
        hBox.setStyle("-fx-border-color: #5bbedc; -fx-border-width: 1px; -fx-border-radius: 5px; -fx-padding: 2px;");

        GridPane gridPane = new GridPane();
        gridPane.setMinSize(500, 350);
        gridPane.setPadding(new Insets(50, 50, 50, 50));
        gridPane.setVgap(5);
        gridPane.setHgap(25);
        gridPane.setAlignment(Pos.CENTER);
        GridPane.setMargin(hBox, new Insets(4, 0, 16, 0));

        // Add all the components to the pane
        // Element for username
        gridPane.add(userLabel, 0, 0);
        gridPane.add(usernameField, 0, 1);

        // Element for firstname
        gridPane.add(firstnameLabel, 0, 2);
        gridPane.add(firstnameField, 0, 3);

        // Element for lastname
        gridPane.add(lastnameLabel, 0, 4);
        gridPane.add(lastnameField, 0, 5);

        // Element for Call to Actions
        gridPane.add(comboBox, 0, 6);
        gridPane.add(hBox, 0, 7);
        gridPane.add(linkToLogin, 0, 8);
        registerButton.setAlignment(Pos.CENTER);
        gridPane.add(registerButton, 0, 9);


        // Button Actions and Events
        btnFileChooser.setOnAction(e -> {
            File selectedFile = fileChooser.showOpenDialog(stage);
            if (selectedFile != null) {
                fileNameLabel.setText(selectedFile.getName());
                userProfileImagePath = selectedFile.getAbsolutePath();
            }
        });

        registerButton.setOnAction(e -> {
            try {
                attemptRegister(e);
            } catch (IOException ex) {
                var alert = new Alert(Alert.AlertType.ERROR, "Something went wrong, please try again");
                System.out.println(ex.getMessage());
                alert.setTitle("Registration Error");
                alert.showAndWait();
            }
        });

        hyperlink.setOnAction(e -> {
            try {
                goToLogin(e);
            } catch (IOException ex) {
                var alert = new Alert(Alert.AlertType.ERROR, "Something went wrong, please try again");
                System.out.println(ex.getMessage());
                alert.setTitle("Registration Error");
                alert.showAndWait();
            }

        });

        base.getChildren().add(gridPane);
        base.setAlignment(Pos.CENTER);
    }

    private Pair<Boolean, String> validate(Map<String, String> userDetails) {
        boolean errorEncountered = false;
        String errorMessage = "";
        String registrationType = userDetails.get("registrationType");
        String firstname = userDetails.get("firstname");
        String lastname = userDetails.get("lastname");
        String username = userDetails.get("username");
        if (registrationType == null) {
            errorEncountered = true;
            errorMessage += "Please select a registration type";
            errorMessage += "\n";
        }
        if (username == null || username.isEmpty()) {
            errorEncountered = true;
            errorMessage += "Please provide a valid username";
            errorMessage += "\n";
        }
        if (firstname == null || firstname.isEmpty()) {
            errorEncountered = true;
            errorMessage += "Please provide a valid firstname";
            errorMessage += "\n";
        }
        if (lastname == null || lastname.isEmpty()) {
            errorEncountered = true;
            errorMessage += "Please provide a valid username";
            errorMessage += "\n";
        }
        return new Pair<>(errorEncountered, errorMessage);
    }

    private void attemptRegister(ActionEvent event) throws IOException {
        String registrationType = comboBox.getValue();
        String username = usernameField.getText();
        String firstname = firstnameField.getText();
        String lastname = lastnameField.getText();
        Map<String, String> userDetails = new HashMap<>();
        userDetails.put("registrationType", registrationType);
        userDetails.put("username", username);
        userDetails.put("firstname", firstname);
        userDetails.put("lastname", lastname);
        var validationResult = validate(userDetails);
        Boolean errorEncountered = validationResult.getKey();
        if (errorEncountered) {
            var errorMessage = validationResult.getValue();
            var alert = new Alert(Alert.AlertType.ERROR, errorMessage);
            alert.setTitle("Registration Error");
            alert.showAndWait();
            return;
        }

        if (registrationType.equalsIgnoreCase("Register as Admin")) {
            Admin admin = new Admin(username, firstname, lastname, userProfileImagePath);
        } else if (registrationType.equalsIgnoreCase("Register as User")) {
            Player player = new Player(username, firstname, lastname, userProfileImagePath);
            var userRepo = UserRepository.getInstance();
            try {
                userRepo.savePlayer(player);
            } catch (UserExistsException e) {
                var alert = new Alert(Alert.AlertType.ERROR, e.getMessage());
                alert.setTitle("Registration Error");
                alert.showAndWait();
                return;
            }
            FXMLLoader loader = new FXMLLoader(getClass().getResource("player-dashboard-view.fxml"));
            root = loader.load();
            PlayerDashboardController controller = loader.getController();
            controller.setWelcomeText(player.getUsername());
            stage = (Stage) ((Node) event.getSource()).getScene().getWindow();
            scene = new Scene(root);
            stage.setScene(scene);
            stage.show();
        }
    }


    private void goToLogin(ActionEvent event) throws IOException {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("login-view.fxml"));
        root = loader.load();
        stage = (Stage) ((Node) event.getSource()).getScene().getWindow();
        scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
}