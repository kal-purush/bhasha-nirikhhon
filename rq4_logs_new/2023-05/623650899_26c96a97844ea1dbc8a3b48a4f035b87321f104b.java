package MainStage;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

import java.net.URL;

public class MainStage extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception {
        FXMLLoader loader=new FXMLLoader();

        URL mainFXML=getClass().getResource("/MainStage/Components/Main/MainStepper.fxml");
        loader.setLocation(mainFXML);
        BorderPane root=loader.load();



        primaryStage.setTitle("Stepper");
        Scene scene=new Scene(root,1050, 600);
        primaryStage.setScene(scene);
        primaryStage.show();

    }

    public void run(String[] args){
        launch(args);
    }
}