package myInvoices;

import javafx.application.Application;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class App extends Application {
    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("/company.fxml"));
        Scene scene = new Scene(root);
        scene.getStylesheets().add("/style.css");
        stage.setScene(scene);
        stage.show();
    }
}