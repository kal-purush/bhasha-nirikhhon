package tictactoe.ui;

import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.RowConstraints;
import javafx.stage.Stage;

public  class NewGame1Base extends BorderPane {

    protected final GridPane gridPane;
    protected final ColumnConstraints columnConstraints;
    protected final RowConstraints rowConstraints;
    protected final RowConstraints rowConstraints0;
    protected final RowConstraints rowConstraints1;
    protected final RowConstraints rowConstraints2;
    protected final Button LogOut;
    protected final ImageView Avater;
    protected final Label label;
    protected final Label label0;
    protected final ImageView imageView;
    protected final Button NEWGAME;
    protected final Button History;
        protected Stage mystage;

    public NewGame1Base(Stage mystage) {
        this.mystage=mystage;
        gridPane = new GridPane();
        columnConstraints = new ColumnConstraints();
        rowConstraints = new RowConstraints();
        rowConstraints0 = new RowConstraints();
        rowConstraints1 = new RowConstraints();
        rowConstraints2 = new RowConstraints();
        LogOut = new Button();
        Avater = new ImageView();
        label = new Label();
        label0 = new Label();
        imageView = new ImageView();
        NEWGAME = new Button();
        History = new Button();
        this.getStyleClass().add("border-pane");

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(600.0);
        setPrefWidth(800.0);

        BorderPane.setAlignment(gridPane, javafx.geometry.Pos.CENTER);
        gridPane.setPrefHeight(558.0);
        gridPane.setPrefWidth(800.0);

        columnConstraints.setHgrow(javafx.scene.layout.Priority.SOMETIMES);
        columnConstraints.setMinWidth(10.0);
        columnConstraints.setPrefWidth(100.0);

        rowConstraints.setMaxHeight(184.0);
        rowConstraints.setMinHeight(10.0);
        rowConstraints.setPrefHeight(100.0);
        rowConstraints.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints0.setMaxHeight(277.0);
        rowConstraints0.setMinHeight(10.0);
        rowConstraints0.setPrefHeight(138.0);
        rowConstraints0.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints1.setMaxHeight(321.0);
        rowConstraints1.setMinHeight(10.0);
        rowConstraints1.setPrefHeight(130.0);
        rowConstraints1.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        rowConstraints2.setMaxHeight(194.0);
        rowConstraints2.setMinHeight(10.0);
        rowConstraints2.setPrefHeight(194.0);
        rowConstraints2.setVgrow(javafx.scene.layout.Priority.SOMETIMES);

        LogOut.setMnemonicParsing(false);
        LogOut.setText("");
//        ImageView imageView0 = new ImageView(new Image(getClass().getResource("/resources/img/logout.png").toExternalForm()));
//        imageView0.setFitWidth(75);
//        imageView0.setFitHeight(75);
//        LogOut.setGraphic(imageView0);
        LogOut.setId("LogOut");
        GridPane.setMargin(LogOut, new Insets(0.0, 20.0, 0.0, 680.0));

       // Avater.setImage(new Image(getClass().getResource("/resources/img/hacker.png").toExternalForm()));
        GridPane.setMargin(Avater, new Insets(0.0, 0.0, 0.0, 25.0));

        label.setText("Ali Kotb");
        label.setId("UserName ");
        GridPane.setMargin(label, new Insets(0.0, 0.0, 25.0, 125.0));

        label0.setText("777");
        label0.setId("score");
        GridPane.setMargin(label0, new Insets(50.0, 0.0, 0.0, 125.0));

        GridPane.setRowIndex(imageView, 1);
//        imageView.setFitHeight(118.0);
//        imageView.setFitWidth(308.0);
//        imageView.setImage(new Image(getClass().getResource("/resources/img/logo.png").toExternalForm()));
        GridPane.setMargin(imageView, new Insets(0.0, 0.0, 0.0, 246.0));

        GridPane.setRowIndex(NEWGAME, 2);
        NEWGAME.setMnemonicParsing(false);
        NEWGAME.setPrefHeight(90.0);
        NEWGAME.setPrefWidth(480.0);
        NEWGAME.setText("NEW GAME");
        NEWGAME.setId("NEWGAME");

        GridPane.setMargin(NEWGAME, new Insets(0.0, 0.0, 0.0, 160.0));

        GridPane.setRowIndex(History, 3);
        History.setMnemonicParsing(false);
        History.setPrefHeight(90.0);
        History.setPrefWidth(480.0);
        History.setText("HISTORY");
        History.setId("HISTORY");

        GridPane.setMargin(History, new Insets(0.0, 0.0, 100.0, 160.0));
        setCenter(gridPane);

        gridPane.getColumnConstraints().add(columnConstraints);
        gridPane.getRowConstraints().add(rowConstraints);
        gridPane.getRowConstraints().add(rowConstraints0);
        gridPane.getRowConstraints().add(rowConstraints1);
        gridPane.getRowConstraints().add(rowConstraints2);
        gridPane.getChildren().add(LogOut);
        gridPane.getChildren().add(Avater);
        gridPane.getChildren().add(label);
        gridPane.getChildren().add(label0);
        gridPane.getChildren().add(imageView);
        gridPane.getChildren().add(NEWGAME);
        gridPane.getChildren().add(History);

    }
}