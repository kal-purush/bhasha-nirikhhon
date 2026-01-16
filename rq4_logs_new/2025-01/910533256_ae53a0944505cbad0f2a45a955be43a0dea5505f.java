package tictactoe.ui;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.RowConstraints;
import javafx.scene.text.Font;
import javafx.stage.Stage;

public class LogInBase extends BorderPane {

    protected final GridPane gridPane;
    protected final ColumnConstraints columnConstraints;
    protected final RowConstraints rowConstraints;
    protected final RowConstraints rowConstraints0;
    protected final RowConstraints rowConstraints1;
    protected final RowConstraints rowConstraints2;
    protected final RowConstraints rowConstraints3;
    protected final ImageView imageView;
    protected final Button back;
    protected final TextField Username;
    protected final TextField Password;
    protected final Button Login;
    protected final Label label;
    protected final Button Login2;
    protected Stage mystage;

    public LogInBase(Stage mystage) {
        this.mystage = mystage;
        gridPane = new GridPane();
        columnConstraints = new ColumnConstraints();
        rowConstraints = new RowConstraints();
        rowConstraints0 = new RowConstraints();
        rowConstraints1 = new RowConstraints();
        rowConstraints2 = new RowConstraints();
        rowConstraints3 = new RowConstraints();
        imageView = new ImageView();
        back = new Button();
        Username = new TextField();
        Password = new TextField();
        Login = new Button();
        label = new Label();
        Login2 = new Button();
        mystage.setResizable(false);

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(600.0);
        setPrefWidth(800.0);

        BorderPane.setAlignment(gridPane, javafx.geometry.Pos.CENTER);
        this.getStyleClass().add("border-pane");

        columnConstraints.setHgrow(javafx.scene.layout.Priority.SOMETIMES);
        columnConstraints.setMinWidth(10.0);
        columnConstraints.setPrefWidth(100.0);

        rowConstraints.setMaxHeight(258.0);
        rowConstraints.setMinHeight(10.0);
        rowConstraints.setPrefHeight(181.0);
        rowConstraints.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints0.setMaxHeight(266.0);
        rowConstraints0.setMinHeight(10.0);
        rowConstraints0.setPrefHeight(121.0);
        rowConstraints0.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints1.setMaxHeight(195.0);
        rowConstraints1.setMinHeight(10.0);
        rowConstraints1.setPrefHeight(99.0);
        rowConstraints1.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints2.setMaxHeight(181.0);
        rowConstraints2.setMinHeight(10.0);
        rowConstraints2.setPrefHeight(68.0);
        rowConstraints2.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints3.setMaxHeight(111.0);
        rowConstraints3.setMinHeight(10.0);
        rowConstraints3.setPrefHeight(111.0);
        rowConstraints3.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        imageView.setFitHeight(118.0);
        imageView.setFitWidth(308.0);
        // imageView.setImage(new Image(getClass().getResource("/resources/img/logo.png").toExternalForm()));
        GridPane.setMargin(imageView, new Insets(100.0, 0.0, 0.0, 246.0));

        back.setMnemonicParsing(false);
        back.setId("back");
        back.setText("");
        back.setOnAction((ActionEvent event) -> {
            mystage.setScene(new Scene(new Home(mystage), 800, 600));

        });

        GridPane.setMargin(back, new Insets(0.0, 0.0, 75.0, 37.0));

        GridPane.setRowIndex(Username, 1);
        Username.setMaxHeight(60.0);
        Username.setMaxWidth(480.0);
        Username.setPrefHeight(60.0);
        Username.setPrefWidth(480.0);
        Username.setPromptText("Username");
        Username.setId("UsernameTxt");
        GridPane.setMargin(Username, new Insets(50.0, 0.0, 0.0, 160.0));
        Username.setFont(new Font(25.0));

        GridPane.setRowIndex(Password, 2);
        Password.setMaxHeight(60.0);
        Password.setMaxWidth(480.0);
        Password.setPromptText("Password");
        Password.setId("PasswordTxt");
        GridPane.setMargin(Password, new Insets(0.0, 0.0, 0.0, 160.0));
        Password.setFont(new Font(25.0));

        GridPane.setRowIndex(Login, 3);
        Login.setMaxHeight(80.0);
        Login.setMaxWidth(200.0);
        Login.setMnemonicParsing(false);
        Login.setText("LOGIN");
        Login.setId("Login");
        GridPane.setMargin(Login, new Insets(0.0, 0.0, 0.0, 300.0));

        GridPane.setRowIndex(label, 4);
        label.setPrefHeight(31.0);
        label.setPrefWidth(260.0);
        label.setText("Don't Have Account?");
        label.setId("label");
        GridPane.setMargin(label, new Insets(0.0, 0.0, 0.0, 225.0));

        GridPane.setRowIndex(Login2, 4);
        Login2.setMnemonicParsing(false);
        Login2.setPrefHeight(31.0);
        Login2.setPrefWidth(100.0);
        Login2.setText("SIGN UP");
        Login2.setId("Login2");
        Login2.setOnAction((ActionEvent event) -> {
            mystage.setScene(new Scene(new SignUp(mystage), 800, 600));

        });

        GridPane.setMargin(Login2, new Insets(0.0, 0.0, 0.0, 475.0));
        Login2.setPadding(new Insets(0.0, 15.0, 0.0, 0.0));
        setCenter(gridPane);

        gridPane.getColumnConstraints().add(columnConstraints);
        gridPane.getRowConstraints().add(rowConstraints);
        gridPane.getRowConstraints().add(rowConstraints0);
        gridPane.getRowConstraints().add(rowConstraints1);
        gridPane.getRowConstraints().add(rowConstraints2);
        gridPane.getRowConstraints().add(rowConstraints3);
        gridPane.getChildren().add(imageView);
        gridPane.getChildren().add(back);
        gridPane.getChildren().add(Username);
        gridPane.getChildren().add(Password);
        gridPane.getChildren().add(Login);
        gridPane.getChildren().add(label);
        gridPane.getChildren().add(Login2);

    }
}