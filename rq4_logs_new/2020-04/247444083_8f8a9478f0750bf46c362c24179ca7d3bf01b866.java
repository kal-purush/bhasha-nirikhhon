package main;


import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

public class UserInterface extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Flower tamagotchi");
        Group root = new Group();

        Scene scene = new Scene(root);

        int width = 1000; // max hp
        GridPane hp = new GridPane();
        //hp.setPrefSize(width, 30);
        hp.getColumnConstraints().add(new ColumnConstraints(width*0.1)); // dead zone red
        hp.getColumnConstraints().add(new ColumnConstraints(width*0.55)); // normal zone yellow-green
        hp.getColumnConstraints().add(new ColumnConstraints(width*0.35)); // perfect zone green


        Rectangle deadzone = new Rectangle(width*0.1, 30, Color.rgb(255, 80, 80, 0.85));
        Rectangle normalzone = new Rectangle(width*0.55, 30, Color.rgb(204, 255, 51));
        Rectangle perfzone = new Rectangle(width*0.35, 30, Color.rgb(102, 255, 102));

        hp.add(deadzone,0,0);
        hp.add(normalzone,1,0);
        hp.add(perfzone,2,0);

        root.getChildren().add(hp);

        Button stratButton = new Button("Strat");
        Button newgameButton = new Button("New game");

        Button addButton = new Button("Add resources");
        Button checkButton = new Button("Check stata");

        Button addWater = new Button("Wather");
        Button addFood = new Button("Food");
        Button addLove = new Button("Love");

        primaryStage.setScene(scene);
        scene.setFill(Color.rgb(0, 102, 102));
        primaryStage.show();

    }
}