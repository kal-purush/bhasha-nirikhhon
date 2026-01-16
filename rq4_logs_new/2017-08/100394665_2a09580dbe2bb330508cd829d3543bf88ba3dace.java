package sample;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.scene.control.Button;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;

public class Main extends Application {
    Stage window;
    Scene scene1,scene2;
    Button button1;

    public static void main(String[] args) {
        launch(args);
    }
    @Override

    public void start(Stage primaryStage) throws Exception{
        window=primaryStage;
        window.setTitle("Chice Menu");
        button1 = new Button("Select");
        ChoiceBox<String> choiceBox = new ChoiceBox<>();
        choiceBox.getItems().add("Semester");
        choiceBox.getItems().addAll("Course","Teacher","Assignment","Exam");
        choiceBox.setValue("Semester");
        button1.setOnAction(e -> getChoice(choiceBox));


        //Label label1 = new Label("Welcome to first scene");
        //Button button1=new Button("Go to second Window");
        VBox layout1 = new VBox(20);
        layout1.setPadding(new Insets(30,30,30,30));
        layout1.getChildren().addAll(choiceBox,button1);
        scene1 = new Scene(layout1,500,500);
        //Button button2=new Button("Go back to first Window");
        //button2.setOnAction(e -> window.setScene(scene1));
       // StackPane layout2 =new StackPane();
       // layout2.getChildren().addAll(button2);
        //scene2 = new Scene(layout2,400,600);
        window.setScene(scene1);
        window.show();



    }
    private void getChoice(ChoiceBox<String> choiceBox)
    {
        String select = choiceBox.getValue();
        Button button2;
        ChoiceBox<String> choiceBox2 = new ChoiceBox<>();
        choiceBox2.getItems().addAll("Spring","Summer","Winter");
        button2=new Button("Select semester name");
        choiceBox2.setValue("Spring");
        button2.setOnAction(e ->
        {
            System.out.println("you select "+choiceBox2.getValue());
        });
        VBox layout2 = new VBox(20);
        layout2.setPadding(new Insets(30,30,30,30));
        layout2.getChildren().addAll(choiceBox2,button2);
        scene2 = new Scene(layout2,600,400);
        window.setTitle(select);
        window.setScene(scene2);
    }

}