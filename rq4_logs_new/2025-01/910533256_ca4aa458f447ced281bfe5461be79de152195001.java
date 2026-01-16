package tictactoe;

import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.RowConstraints;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.stage.Stage;

public class SignUp extends BorderPane {

    protected final GridPane gridPane;
    protected final ColumnConstraints columnConstraints;
    protected final RowConstraints rowConstraints;
    protected final RowConstraints rowConstraints0;
    protected final RowConstraints rowConstraints1;
    protected final RowConstraints rowConstraints2;
    protected final TextField username;
    protected final TextField email;
    protected final TextField password;
    protected final TextField confirmPassword;
    protected final VBox vBox;
    protected final Button signUpButton;
    protected final FlowPane flowPane;
    protected final Text text;
    protected final Button LogInButton;
    protected final FlowPane flowPane0;
    protected final Button backButton;
    protected final ImageView backIcon;
    protected final ImageView imageView;

    public SignUp(Stage stage) {

        gridPane = new GridPane();
        columnConstraints = new ColumnConstraints();
        rowConstraints = new RowConstraints();
        rowConstraints0 = new RowConstraints();
        rowConstraints1 = new RowConstraints();
        rowConstraints2 = new RowConstraints();
        username = new TextField();
        email = new TextField();
        password = new TextField();
        confirmPassword = new TextField();
        vBox = new VBox();
        signUpButton = new Button();
        flowPane = new FlowPane();
        text = new Text();
        LogInButton = new Button();
        flowPane0 = new FlowPane();
        backButton = new Button();
        backIcon = new ImageView();
        imageView = new ImageView();

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(400.0);
        setPrefWidth(600.0);

        BorderPane.setAlignment(gridPane, javafx.geometry.Pos.CENTER);

        columnConstraints.setHgrow(javafx.scene.layout.Priority.SOMETIMES);
        columnConstraints.setMinWidth(10.0);
        columnConstraints.setPrefWidth(100.0);

        rowConstraints.setMinHeight(10.0);
        rowConstraints.setPrefHeight(30.0);
        rowConstraints.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints0.setMinHeight(10.0);
        rowConstraints0.setPrefHeight(30.0);
        rowConstraints0.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints1.setMinHeight(10.0);
        rowConstraints1.setPrefHeight(30.0);
        rowConstraints1.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints2.setMinHeight(10.0);
        rowConstraints2.setPrefHeight(30.0);
        rowConstraints2.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        username.setPromptText("Username");
        username.setFont(new Font("Arial Italic", 17.0));
        username.setOpaqueInsets(new Insets(0.0));
        GridPane.setMargin(username, new Insets(0.0, 80.0, 0.0, 80.0));

        GridPane.setRowIndex(email, 1);
        email.setPromptText("Email");
        GridPane.setMargin(email, new Insets(0.0, 80.0, 0.0, 80.0));
        email.setFont(new Font("Arial Italic", 17.0));

        GridPane.setRowIndex(password, 2);
        password.setPromptText("Password");
        GridPane.setMargin(password, new Insets(0.0, 80.0, 0.0, 80.0));
        password.setFont(new Font("Arial Italic", 17.0));

        GridPane.setRowIndex(confirmPassword, 3);
        confirmPassword.setPromptText("Confirm password");
        GridPane.setMargin(confirmPassword, new Insets(0.0, 80.0, 0.0, 80.0));
        confirmPassword.setFont(new Font("Arial Italic", 17.0));
        setCenter(gridPane);

        BorderPane.setAlignment(vBox, javafx.geometry.Pos.CENTER);
        vBox.setPrefHeight(89.0);
        vBox.setPrefWidth(605.0);

        signUpButton.setMnemonicParsing(false);
        signUpButton.setPrefHeight(25.0);
        signUpButton.setPrefWidth(141.0);
        signUpButton.setText("SIGN UP");
        signUpButton.setFont(new Font(16.0));
        VBox.setMargin(signUpButton, new Insets(0.0, 0.0, 0.0, 230.0));

        flowPane.setPrefHeight(200.0);
        flowPane.setPrefWidth(200.0);

        text.setStrokeType(javafx.scene.shape.StrokeType.OUTSIDE);
        text.setStrokeWidth(0.0);
        text.setText("Already Have An Account?");
        text.setFont(Font.font("Black Han Sans", FontWeight.BOLD, 18));
        text.setFill(Color.WHITE);
        FlowPane.setMargin(text, new Insets(0.0, 0.0, 0.0, 230.0));

        LogInButton.setMnemonicParsing(false);
        LogInButton.setText("Log in");
        FlowPane.setMargin(LogInButton, new Insets(0.0, 0.0, 0.0, 20.0));
        VBox.setMargin(flowPane, new Insets(0.0));
        setBottom(vBox);

        BorderPane.setAlignment(flowPane0, javafx.geometry.Pos.CENTER);
        flowPane0.setPrefHeight(59.0);
        flowPane0.setPrefWidth(600.0);

        backButton.setMnemonicParsing(false);
        backButton.setPrefHeight(42.0);
        backButton.setPrefWidth(50.0);

        backIcon.setDisable(true);
        backIcon.setFitHeight(48.0);
        backIcon.setFitWidth(50.0);
        backIcon.setImage(new Image(getClass().getResource("/resources/images/backIcon.png").toExternalForm()));
        backButton.setGraphic(backIcon);

        imageView.setFitHeight(57.0);
        imageView.setFitWidth(171.0);
        imageView.setImage(new Image(getClass().getResource("/resources/images/logo.png").toExternalForm()));
        FlowPane.setMargin(imageView, new Insets(0.0, 0.0, 0.0, 150.0));
        setTop(flowPane0);

        username.setId("username");
        email.setId("email");
        password.setId("password");
        confirmPassword.setId("confirmPassword");
        signUpButton.setId("signUpButton");
        LogInButton.setId("LogInButton");
        imageView.setId("logoImage ");
        backButton.setId("backButton");

      //  this.getStylesheets().add(getClass().getResource("/CSS/homeStyle.css").toExternalForm());

        gridPane.getColumnConstraints().add(columnConstraints);
        gridPane.getRowConstraints().add(rowConstraints);
        gridPane.getRowConstraints().add(rowConstraints0);
        gridPane.getRowConstraints().add(rowConstraints1);
        gridPane.getRowConstraints().add(rowConstraints2);
        gridPane.getChildren().add(username);
        gridPane.getChildren().add(email);
        gridPane.getChildren().add(password);
        gridPane.getChildren().add(confirmPassword);
        vBox.getChildren().add(signUpButton);
        flowPane.getChildren().add(text);
        flowPane.getChildren().add(LogInButton);
        vBox.getChildren().add(flowPane);
        flowPane0.getChildren().add(backButton);
        flowPane0.getChildren().add(imageView);

    }
}