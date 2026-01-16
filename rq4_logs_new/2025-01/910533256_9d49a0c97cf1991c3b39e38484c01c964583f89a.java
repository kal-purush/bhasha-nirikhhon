package tictactoe.ui;

import javafx.geometry.Insets;
import javafx.scene.Cursor;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Line;
import javafx.scene.text.Font;

public class Board extends BorderPane {

    protected final ImageView imageView;
    protected final FlowPane flowPaneCenter;
    protected final FlowPane flowPaneRow1;
    protected final Button tile1;
    protected final Line line1;
    protected final Button tile2;
    protected final Line line2;
    protected final Button tile3;
    protected final Line line3;
    protected final FlowPane flowPaneRow2;
    protected final Button tile4;
    protected final Line line4;
    protected final Button tile5;
    protected final Line line5;
    protected final Button tile6;
    protected final Line line6;
    protected final FlowPane flowPaneRow3;
    protected final Button tile7;
    protected final Line line7;
    protected final Button tile8;
    protected final Line line8;
    protected final Button tile9;
    protected final VBox vbPlayer2;
    protected final ImageView ivPlayer2;
    protected final Label userNamePlayer2;
    protected final VBox vbPlayer1;
    protected final ImageView ivPlayer1;
    protected final Label userNamePlayer1;
    public static final int MODE_PC = 1;
    public static final int MODE_PLAYER = 2;
    public static final int MODE_ONLINE = 3;

    public Board(int mode) {

        imageView = new ImageView(new Image("/resources/images/logo.png"));
        flowPaneCenter = new FlowPane();
        flowPaneRow1 = new FlowPane();
        tile1 = new Button();
        line1 = new Line();
        tile2 = new Button();
        line2 = new Line();
        tile3 = new Button();
        line3 = new Line();
        flowPaneRow2 = new FlowPane();
        tile4 = new Button();
        line4 = new Line();
        tile5 = new Button();
        line5 = new Line();
        tile6 = new Button();
        line6 = new Line();
        flowPaneRow3 = new FlowPane();
        tile7 = new Button();
        line7 = new Line();
        tile8 = new Button();
        line8 = new Line();
        tile9 = new Button();
        vbPlayer2 = new VBox();
        ivPlayer2 = new ImageView(new Image("/resources/images/player1.png"));
        userNamePlayer2 = new Label();
        vbPlayer1 = new VBox();
        ivPlayer1 = new ImageView(new Image("/resources/images/player1.png"));
        userNamePlayer1 = new Label();

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(600.0);
        setPrefWidth(800.0);

        BorderPane.setAlignment(imageView, javafx.geometry.Pos.TOP_CENTER);
        imageView.setFitHeight(108.0);
        imageView.setFitWidth(308.0);
        imageView.setPickOnBounds(true);
        imageView.setPreserveRatio(true);
        BorderPane.setMargin(imageView, new Insets(70.0, 0.0, 0.0, 0.0));
//        imageView.setImage(new Image(getClass().getResource("../images/logo.png").toExternalForm()));
        setTop(imageView);

        BorderPane.setAlignment(flowPaneCenter, javafx.geometry.Pos.CENTER);
        flowPaneCenter.setAlignment(javafx.geometry.Pos.CENTER);
        flowPaneCenter.setOrientation(javafx.geometry.Orientation.VERTICAL);
        flowPaneCenter.setPrefHeight(300.0);
        flowPaneCenter.setPrefWidth(320.0);

        flowPaneRow1.setPrefHeight(100.0);
        flowPaneRow1.setPrefWidth(320.0);

        tile1.setMnemonicParsing(false);
        tile1.setPrefHeight(100.0);
        tile1.setPrefWidth(100.0);
        tile1.setCursor(Cursor.HAND);
        tile1.setBackground(null);

        line1.setEndY(250.0);
        line1.setStartY(160.0);
        line1.setStroke(javafx.scene.paint.Color.WHITE);
        line1.setStrokeWidth(10.0);

        tile2.setMnemonicParsing(false);
        tile2.setPrefHeight(100.0);
        tile2.setPrefWidth(100.0);
        tile2.setCursor(Cursor.HAND);
        tile2.setBackground(null);

        line2.setEndY(250.0);
        line2.setStartY(160.0);
        line2.setStroke(javafx.scene.paint.Color.WHITE);
        line2.setStrokeWidth(10.0);

        tile3.setMnemonicParsing(false);
        tile3.setPrefHeight(100.0);
        tile3.setPrefWidth(100.0);
        tile3.setCursor(Cursor.HAND);
        tile3.setBackground(null);

        line3.setEndX(550.0);
        line3.setStartX(240.0);
        line3.setStroke(javafx.scene.paint.Color.WHITE);
        line3.setStrokeWidth(10.0);

        flowPaneRow2.setPrefHeight(100.0);
        flowPaneRow2.setPrefWidth(320.0);

        tile4.setMnemonicParsing(false);
        tile4.setPrefHeight(100.0);
        tile4.setPrefWidth(100.0);
        tile4.setCursor(Cursor.HAND);
        tile4.setBackground(null);

        line4.setEndY(250.0);
        line4.setStartY(160.0);
        line4.setStroke(javafx.scene.paint.Color.WHITE);
        line4.setStrokeWidth(10.0);

        tile5.setMnemonicParsing(false);
        tile5.setPrefHeight(100.0);
        tile5.setPrefWidth(100.0);
        tile5.setCursor(Cursor.HAND);
        tile5.setBackground(null);

        line5.setEndY(250.0);
        line5.setStartY(160.0);
        line5.setStroke(javafx.scene.paint.Color.WHITE);
        line5.setStrokeWidth(10.0);

        tile6.setMnemonicParsing(false);
        tile6.setPrefHeight(100.0);
        tile6.setPrefWidth(100.0);
        tile6.setCursor(Cursor.HAND);
        tile6.setBackground(null);

        line6.setEndX(550.0);
        line6.setStartX(240.0);
        line6.setStroke(javafx.scene.paint.Color.WHITE);
        line6.setStrokeWidth(10.0);

        flowPaneRow3.setPrefHeight(100.0);
        flowPaneRow3.setPrefWidth(320.0);

        tile7.setMnemonicParsing(false);
        tile7.setPrefHeight(100.0);
        tile7.setPrefWidth(100.0);
        tile7.setCursor(Cursor.HAND);
        tile7.setBackground(null);

        line7.setEndY(250.0);
        line7.setStartY(160.0);
        line7.setStroke(javafx.scene.paint.Color.WHITE);
        line7.setStrokeWidth(10.0);

        tile8.setMnemonicParsing(false);
        tile8.setPrefHeight(100.0);
        tile8.setPrefWidth(100.0);
        tile8.setCursor(Cursor.HAND);
        tile8.setBackground(null);

        line8.setEndY(250.0);
        line8.setStartY(160.0);
        line8.setStroke(javafx.scene.paint.Color.WHITE);
        line8.setStrokeWidth(10.0);

        tile9.setMnemonicParsing(false);
        tile9.setPrefHeight(100.0);
        tile9.setPrefWidth(100.0);
        tile9.setCursor(Cursor.HAND);
        tile9.setBackground(null);
        setCenter(flowPaneCenter);

        BorderPane.setAlignment(vbPlayer2, javafx.geometry.Pos.CENTER);
        vbPlayer2.setAlignment(javafx.geometry.Pos.TOP_CENTER);
        vbPlayer2.setPrefHeight(200.0);
        vbPlayer2.setPrefWidth(240.0);
        BorderPane.setMargin(vbPlayer2, new Insets(36.0, 0.0, 0.0, 0.0));

        ivPlayer2.setFitHeight(120.0);
        ivPlayer2.setFitWidth(120.0);
        ivPlayer2.setPickOnBounds(true);
        ivPlayer2.setPreserveRatio(true);
//        ivPlayer2.setImage(new Image(getClass().getResource("../images/player1.png").toExternalForm()));
        VBox.setMargin(ivPlayer2, new Insets(0.0, 0.0, 15.0, 0.0));
//        Font font = Font.loadFont(getClass().getResourceAsStream("/fonts/MyCustomFont.ttf"), 25.0);  // 16px size

        if (mode == MODE_PC) {
            userNamePlayer1.setText("Player 1");
            userNamePlayer2.setText("PC");
        } else if (mode == MODE_PLAYER) {
            userNamePlayer1.setText("Player 1");
            userNamePlayer2.setText("Player 2");
        } else if (mode == MODE_ONLINE) {
            userNamePlayer1.setText("Yousef");
            userNamePlayer2.setText("Ali");
        }
        userNamePlayer2.setTextFill(javafx.scene.paint.Color.WHITE);
        userNamePlayer2.setWrapText(true);
//        userNamePlayer2.setFont(font);
        setRight(vbPlayer2);

        BorderPane.setAlignment(vbPlayer1, javafx.geometry.Pos.CENTER);
        vbPlayer1.setAlignment(javafx.geometry.Pos.TOP_CENTER);
        vbPlayer1.setPrefHeight(200.0);
        vbPlayer1.setPrefWidth(240.0);

        ivPlayer1.setFitHeight(120.0);
        ivPlayer1.setFitWidth(120.0);
        ivPlayer1.setPickOnBounds(true);
        ivPlayer1.setPreserveRatio(true);
//        ivPlayer1.setImage(new Image(getClass().getResource("../images/player1.png").toExternalForm()));
        VBox.setMargin(ivPlayer1, new Insets(0.0, 0.0, 15.0, 0.0));

        userNamePlayer1.setTextFill(javafx.scene.paint.Color.WHITE);
//        userNamePlayer1.setFont(font);
        BorderPane.setMargin(vbPlayer1, new Insets(36.0, 0.0, 0.0, 0.0));
        setLeft(vbPlayer1);

        flowPaneRow1.getChildren().add(tile1);
        flowPaneRow1.getChildren().add(line1);
        flowPaneRow1.getChildren().add(tile2);
        flowPaneRow1.getChildren().add(line2);
        flowPaneRow1.getChildren().add(tile3);
        flowPaneCenter.getChildren().add(flowPaneRow1);
        flowPaneCenter.getChildren().add(line3);
        flowPaneRow2.getChildren().add(tile4);
        flowPaneRow2.getChildren().add(line4);
        flowPaneRow2.getChildren().add(tile5);
        flowPaneRow2.getChildren().add(line5);
        flowPaneRow2.getChildren().add(tile6);
        flowPaneCenter.getChildren().add(flowPaneRow2);
        flowPaneCenter.getChildren().add(line6);
        flowPaneRow3.getChildren().add(tile7);
        flowPaneRow3.getChildren().add(line7);
        flowPaneRow3.getChildren().add(tile8);
        flowPaneRow3.getChildren().add(line8);
        flowPaneRow3.getChildren().add(tile9);
        flowPaneCenter.getChildren().add(flowPaneRow3);
        vbPlayer2.getChildren().add(ivPlayer2);
        vbPlayer2.getChildren().add(userNamePlayer2);
        vbPlayer1.getChildren().add(ivPlayer1);
        vbPlayer1.getChildren().add(userNamePlayer1);

    }
}