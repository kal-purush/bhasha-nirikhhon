package com.groupthree.mancala;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.Text;
import javafx.scene.text.TextAlignment;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.IOException;

public class Signup extends Application {
  @Override
  public void start(Stage stage) throws IOException {

    // Create Components
    Text userLabel = new Text("Username");
    TextField userField = new TextField("");

    Text firstnameLabel = new Text("First Name");
    TextField firstnameField = new TextField("");

    Text lastnameLabel = new Text("Last Name");
    TextField lastnameField = new TextField("");

    final ComboBox<String> comboBox = new ComboBox<>(FXCollections.observableArrayList("Register as Admin", "Register as User"));
    comboBox.setPromptText("Select Registration Type");

    FileChooser fileChooser = new FileChooser();
    Button btnFileChooser = new Button("Select File");
    Label fileNameLabel = new Label("Select Profile Image");
    HBox hBox = new HBox(fileNameLabel, btnFileChooser);

    Text signInText = new Text("Have an account? Sign-in here");
    Button btnSignIn = new Button("Sign-In");

    // Styling
    btnSignIn.setMinWidth(200);
    btnSignIn.setStyle("-fx-background-color: #7575ec; -fx-alignment: center; -fx-text-fill: white");
    signInText.setTextAlignment(TextAlignment.CENTER);
    signInText.setFont(Font.font("Verdana", FontPosture.ITALIC, 12));
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
    gridPane.add(userField, 0, 1);

    // Element for firstname
    gridPane.add(firstnameLabel, 0, 2);
    gridPane.add(firstnameField, 0, 3);

    // Element for lastname
    gridPane.add(lastnameLabel, 0, 4);
    gridPane.add(lastnameField, 0, 5);

    // Element for Call to Actions
    gridPane.add(comboBox, 0, 6);
    gridPane.add(hBox, 0, 7);
    gridPane.add(signInText, 0, 8);
    btnSignIn.setAlignment(Pos.CENTER);
    gridPane.add(btnSignIn, 0, 9);

    // Create Scene Object and set to Stage
    Scene scene = new Scene(gridPane);
    stage.setTitle("Sign-up Form");
    stage.setScene(scene);
    stage.show();

    // Button Actions and Events
    btnFileChooser.setOnAction(e -> {
      File selectedFile = fileChooser.showOpenDialog(stage);
      if (selectedFile != null) {
        fileNameLabel.setText(selectedFile.getName());
      }
    });
  }
}