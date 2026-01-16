package com.tsystems;
import javafx.application.Application;
import javafx.stage.Stage;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Line;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.collections.ObservableList;
import java.util.ArrayList;


public class Quizfx extends Application {
    //fx
    //creating a Group object
    Group group = new Group();
    //Creating a Scene by passing the group object, height and width
    Scene scene = new Scene(group ,600, 300);
    //Retrieving the observable list object
    ObservableList list = group.getChildren();

    //csv
    csvReader file = new csvReader();
    ArrayList questionTable = new ArrayList(file.readFile());

    //globals
    ArrayList<String> questionLine = new ArrayList<>();
    int score = 0;

    @Override
    public void start(Stage primaryStage) throws Exception {
        Intro();
        //setting color to the scene
        scene.setFill(Color.BROWN);
        //Setting the title to Stage.
        primaryStage.setTitle("The Ultimate Quiz Challenge");
        //Adding the scene to Stage
        primaryStage.setScene(scene);
        //Displaying the contents of the stage
        primaryStage.show();
    }
    public void createScore(int score){
        Rectangle rectangleScore = new Rectangle();
        rectangleScore.setX(0);
        rectangleScore.setY(0);
        rectangleScore.setHeight(30);
        rectangleScore.setWidth(30);
        rectangleScore.setFill(Color.DEEPPINK);

        Text textScore = new Text();
        textScore.setFont(new Font(20));
        textScore.setX(5);
        textScore.setY(20);
        textScore.setText(String.valueOf(score));

        list.add(rectangleScore);
        list.add(textScore);
    }
    public void createAnswer(int answerX, int answerY, String answerText) {
        //Answer rectangle
        Rectangle rectangleAnswer = new Rectangle();
        Text textAnswer = new Text();

        //Resources to dynamically resize rectangle for smoothly containing the text
        int textSize = answerText.length();
        int textWidth = textSize * 17;
        System.out.println(textSize);
        System.out.println(textWidth);

        //Setting the properties of the rectangle
        rectangleAnswer.setX(answerX);
        rectangleAnswer.setY(answerY);
        if (textWidth > 100) {
            rectangleAnswer.setWidth(textWidth);
        } else {
            rectangleAnswer.setWidth(100.0f);
        }
        rectangleAnswer.setHeight(40.0f);
        //Setting the height and width of the arc
        rectangleAnswer.setArcWidth(30.0);
        rectangleAnswer.setArcHeight(20.0);
        //Color rectangle
        rectangleAnswer.setFill(Color.LIGHTYELLOW);

        //Setting font to the text
        textAnswer.setFont(new Font(20));
        //setting the position of the text
        textAnswer.setX(answerX+10);
        textAnswer.setY(answerY+25);
        //Setting the text to be added.
        textAnswer.setText(answerText);

        EventHandler<MouseEvent> eventHandler = new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent e) {
                System.out.println("Answer clicked!");
                if (questionLine.get(1) == textAnswer.getText()) { score += 1;}
                group.getChildren().clear();
                turnGenerator(questionGenerator(questionTable));
            }
        };

        //Registering the event filter
        rectangleAnswer.addEventFilter(MouseEvent.MOUSE_CLICKED, eventHandler);

        list.add(rectangleAnswer);
        list.add(textAnswer);
    }
    public void createQuestion(String questionText){
        //Question rectangle
        Rectangle rectangleQuestion = new Rectangle();
        //Setting the properties of the rectangle
        rectangleQuestion.setX(140.0f);
        rectangleQuestion.setY(75.0f);
        rectangleQuestion.setWidth(300.0f);
        rectangleQuestion.setHeight(40.0f);
        //Setting the height and width of the arc
        rectangleQuestion.setArcWidth(30.0);
        rectangleQuestion.setArcHeight(20.0);
        //Color rectangle
        rectangleQuestion.setFill(Color.LIGHTYELLOW);

        //Question text
        Text textQuestion = new Text();
        //Setting font to the text
        textQuestion.setFont(new Font(25));
        //setting the position of the text
        textQuestion.setX(150);
        textQuestion.setY(100);
        //Setting the text to be added.
        textQuestion.setText(questionText);

        list.add(rectangleQuestion);
        list.add(textQuestion);
    }

    public void gameOver(){
        group.getChildren().clear();
        Text textGameOver = new Text();
        textGameOver.setX(150);
        textGameOver.setY(100);
        textGameOver.setText("Game Over \n Your score is "+ score);

        Rectangle rectangleGameOver = new Rectangle();
        rectangleGameOver.setX(140.0f);
        rectangleGameOver.setY(75.0f);
        rectangleGameOver.setWidth(300.0f);
        rectangleGameOver.setHeight(120.0f);
        rectangleGameOver.setArcWidth(30.0);
        rectangleGameOver.setArcHeight(20.0);
        rectangleGameOver.setFill(Color.LIGHTYELLOW);

        Text textExit = new Text();
        textExit.setX(180);
        textExit.setY(150);
        textExit.setText("EXIT");

        Rectangle rectangleExit = new Rectangle();
        rectangleExit.setX(175.0f);
        rectangleExit.setY(145.0f);
        rectangleExit.setWidth(30.0f);
        rectangleExit.setHeight(20.0f);
        rectangleExit.setArcWidth(3.0);
        rectangleExit.setArcHeight(2.0);
        //Color rectangle
        rectangleExit.setFill(Color.RED);

        //Creating the mouse event handler
        EventHandler<MouseEvent> eventHandler = new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent e) {
                System.exit(0);
            }
        };

        //Registering the event filter
        rectangleExit.addEventFilter(MouseEvent.MOUSE_CLICKED, eventHandler);


        list.add(rectangleGameOver);
        list.add(textGameOver);
        list.add(rectangleExit);
        list.add(textExit);
    }

    public ArrayList questionGenerator(ArrayList<ArrayList<String>> questionTable){
        if (questionTable.size() > 0){
            questionLine = questionTable.get(0);
            System.out.println(questionLine);
            questionTable.remove(0);
            return questionLine;
        } else {
            ArrayList<String> thisIsTheEnd = new ArrayList<>();
            return thisIsTheEnd;
        }
    }

    public void turnGenerator(ArrayList<String> questionLine){
        if (questionLine.size() == 0) {
            gameOver();
        } else {
            createScore(score);
            createQuestion(questionLine.get(0));
            createAnswer(20, 130, questionLine.get(1));
            createAnswer(280, 130, questionLine.get(2));
            createAnswer(20, 220, questionLine.get(3));
            createAnswer(280, 220, questionLine.get(4));
        }
    }

    public void Intro(){
        //Drawing a Circle
        Circle circleStart = new Circle();
        //Setting the properties of the circle
        circleStart.setCenterX(300.0f);
        circleStart.setCenterY(135.0f);
        circleStart.setRadius(200.0f);
        circleStart.setFill(Color.WHITE);

        //Creating a welcome text object
        Text textStart = new Text();
        //Setting font to the text
        textStart.setFont(new Font(25));
        //setting the position of the text
        textStart.setX(130);
        textStart.setY(150);
        //Setting the text to be added.
        textStart.setText("Click to start a Quiz");

        //Creating a line object
        Line lineStart = new Line();
        //Setting the properties to a line
        lineStart.setStartX(130.0);
        lineStart.setStartY(150.0);
        lineStart.setEndX(460.0);
        lineStart.setEndY(150.0);

        //Creating the mouse event handler
        EventHandler<MouseEvent> eventHandler = new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent e) {
                System.out.println("Fasten your seatbelts!");
                group.getChildren().remove(lineStart);
                group.getChildren().remove(circleStart);
                group.getChildren().remove(textStart);
                scene.setFill(Color.BROWN);
                turnGenerator(questionGenerator(questionTable));
            }
        };
        //Registering the event filter
        circleStart.addEventFilter(MouseEvent.MOUSE_CLICKED, eventHandler);

        //Setting the text object as a node to the group object
        list.add(circleStart);
        list.add(lineStart);
        list.add(textStart);
    }
    public static void main(String[] args){
        launch(args);
    }
}