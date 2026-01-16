package tictactoe;

import java.util.Optional;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.RowConstraints;
import javafx.scene.text.Font;
import javafx.stage.Stage;

public  class Home extends BorderPane {

    protected final GridPane gridPane;
    protected final ColumnConstraints columnConstraints;
    protected final RowConstraints rowConstraints;
    protected final RowConstraints rowConstraints0;
    protected final Button offlineButton;
    protected final Button onlineButton;
    protected final FlowPane flowPane;
    protected final ImageView imageView;

    public Home(Stage stage) {

        gridPane = new GridPane();
        columnConstraints = new ColumnConstraints();
        rowConstraints = new RowConstraints();
        rowConstraints0 = new RowConstraints();
        offlineButton = new Button();
        onlineButton = new Button();
        flowPane = new FlowPane();
        imageView = new ImageView();

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(400.0);
        setPrefWidth(600.0);

        BorderPane.setAlignment(gridPane, javafx.geometry.Pos.CENTER);
        gridPane.setPrefHeight(265.0);
        gridPane.setPrefWidth(600.0);

        columnConstraints.setHgrow(javafx.scene.layout.Priority.SOMETIMES);
        columnConstraints.setMinWidth(10.0);
        columnConstraints.setPrefWidth(100.0);

        rowConstraints.setMinHeight(10.0);
        rowConstraints.setPrefHeight(30.0);
        rowConstraints.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints0.setMinHeight(10.0);
        rowConstraints0.setPrefHeight(30.0);
        rowConstraints0.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        GridPane.setRowSpan(offlineButton, 2);
        offlineButton.setAlignment(javafx.geometry.Pos.CENTER);
        offlineButton.setMinHeight(50.0);
        offlineButton.setMinWidth(80.0);
        offlineButton.setMnemonicParsing(false);
        offlineButton.setPrefHeight(50.0);
        offlineButton.setPrefWidth(190.0);
        offlineButton.setText("OFFLINE");
        GridPane.setMargin(offlineButton, new Insets(0.0, 0.0, 80.0, 170.0));
        offlineButton.setFont(new Font("System Bold", 20.0));


        GridPane.setRowIndex(onlineButton, 1);
        onlineButton.setMinHeight(50.0);
        onlineButton.setMinWidth(80.0);
        onlineButton.setMnemonicParsing(false);
        onlineButton.setPrefHeight(50.0);
        onlineButton.setPrefWidth(190.0);
        onlineButton.setText("ONLINE");
        onlineButton.setTextAlignment(javafx.scene.text.TextAlignment.CENTER);
        GridPane.setMargin(onlineButton, new Insets(0.0, 0.0, 20.0, 170.0));
        onlineButton.setFont(new Font("System Bold", 20.0));
        BorderPane.setMargin(gridPane, new Insets(0.0));
        setCenter(gridPane);
        BorderPane.setAlignment(flowPane, javafx.geometry.Pos.CENTER);
        flowPane.setPrefHeight(73.0);
        flowPane.setPrefWidth(600.0);

        imageView.setFitHeight(76.0);
        imageView.setFitWidth(200.0);
        imageView.setImage(new Image(getClass().getResource("/resources/images/logo.png").toExternalForm()));
        FlowPane.setMargin(imageView, new Insets(0.0, 0.0, 0.0, 200.0));
        setTop(flowPane);
        
        
         offlineButton.setId("offlineBtn");
         onlineButton.setId("onlineBtn");

        gridPane.getColumnConstraints().add(columnConstraints);
        gridPane.getRowConstraints().add(rowConstraints);
        gridPane.getRowConstraints().add(rowConstraints0);
        gridPane.getChildren().add(offlineButton);
        gridPane.getChildren().add(onlineButton);
        flowPane.getChildren().add(imageView);

    }
}