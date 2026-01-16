package com.drinks;

import java.util.List;

import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;

public class DrinksView {

    private int winSize = 400;

    public Stage createMainWindow(Drinks controller, Stage primaryStage) {
        primaryStage.setTitle("What Should I Drink?");

        GridPane gridPane = new GridPane();
        gridPane.setAlignment(Pos.CENTER);
        gridPane.setVgap(10);
        ColumnConstraints columnConstraints = new ColumnConstraints();
        columnConstraints.setHalignment(HPos.CENTER);
        gridPane.getColumnConstraints().add(columnConstraints);

        Label welcomeText = new Label("Welcome to our application. To start press Start button.");
        welcomeText.setWrapText(true);
        gridPane.add(welcomeText, 0, 0);
        Button startButton = new Button("Start");

        startButton.setOnAction(event -> controller.createDroolsSession());
        gridPane.add(startButton, 0, 1);

        Scene scene = new Scene(gridPane, 400, 300);
        primaryStage.setScene(scene);
        primaryStage.show();
        return primaryStage;
    }

    public Stage createQuestionWindow(Drinks controller, String question, String factName, List<String> previousAnswers) {
        ListView<String> answerList = new ListView<String>();
        Button buttonYes = new Button("Yes");
        Button buttonNo = new Button("No");
        Button buttonReturn = new Button("Return");
        Text message = new Text(question);
        Text message2 = new Text("Answers up to now:");
        // Constructing list of previous answers
        for (String s : previousAnswers) {
            answerList.getItems().add(s);
        }

        Stage thisStage = new Stage();

        buttonYes.setOnAction(event -> controller.handleAnswer(question, factName, true, thisStage));
        buttonNo.setOnAction(event -> controller.handleAnswer(question, factName, false, thisStage));
        buttonReturn.setOnAction(event -> controller.handleReturnButton(answerList, thisStage));

        // Creating window
        GridPane layout = new GridPane();
        layout.setAlignment(Pos.CENTER);
        layout.setVgap(10);
        layout.setHgap(10);
        layout.setPadding(new Insets(20));

        layout.add(message2, 0, 0);
        layout.add(answerList, 0, 1);
        layout.add(message, 0, 2);

        HBox buttonsBox = new HBox(10, buttonYes, buttonNo, buttonReturn);
        buttonsBox.setAlignment(Pos.BASELINE_RIGHT);
        layout.add(buttonsBox, 0, 3);

        Scene scene = new Scene(layout, winSize, winSize);
        thisStage.setTitle("Question");
        thisStage.setScene(scene);
        return thisStage;
    }

    public Stage createResultsWindow(Drinks controller, String results, List<String> previousAnswers) {
        // Creating buttons, message and list that will be displayed
        ListView<String> answerList = new ListView<String>();
        Button buttonReturn = new Button("Return");
        Text message2 = new Text("Answers up to now. If you want to return to the last question, just press Return."
                                  + " If you want to return to even earlier question, click on it on the list and then press Return");

        Text message = new Text("So, we've come to it! At last I know, that you should probably drink " + results
                                + ". If it doesn't suit you, you can always return!");
        // Constructing list of previous answers
        for (String s : previousAnswers) {
            answerList.getItems().add(s);
        }

        message.setWrappingWidth(winSize - 20);
        message2.setWrappingWidth(winSize - 20);

        Stage thisStage = new Stage();

        buttonReturn.setOnAction(event -> controller.handleReturnButton(answerList, thisStage));

        // Creating window
        GridPane layout = new GridPane();
        layout.setAlignment(Pos.CENTER);
        layout.setVgap(10);
        layout.setHgap(10);
        layout.setPadding(new Insets(20));

        layout.add(message2, 0, 0);
        layout.add(answerList, 0, 1);
        layout.add(message, 0, 2);
        layout.add(buttonReturn, 0, 3);
        GridPane.setHalignment(buttonReturn, HPos.RIGHT);

        Scene scene = new Scene(layout, winSize, winSize);
        thisStage.setTitle("Question");
        thisStage.setScene(scene);
        return thisStage;
    }
}