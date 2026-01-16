package learn.ijpds2nd.ch14fx;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

public class ButtonInPane extends Application {
    @Override
    public void start(Stage primaryStage) {
        Button btOk = new Button("Ok");
        Pane pane = new StackPane();
        pane.getChildren().add(btOk);
        primaryStage.setScene(new Scene(pane, 240,50));
        primaryStage.setTitle("ButtonInPane");
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}