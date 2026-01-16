package hu.unideb.inf;

import java.sql.*;

import java.sql.SQLException;

import javafx.application.Application;


import javafx.fxml.FXMLLoader;

import javafx.scene.Scene;

import javafx.stage.Stage;

public class MainApp extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        FXMLLoader loader = new FXMLLoader(MainApp.class.getResource("/fxml/FXMLScene.fxml"));
        Scene scene = new Scene(loader.load());
        stage.setTitle("PC-Builder");
        stage.setScene(scene);
        stage.show();

    }
    /**
     * The main() method is ignored in correctly deployed JavaFX application.main() serves only as fallback in case the application can not be
     launched through deployment artifacts, e.g., in IDEs with limited FX
     support.
     * NetBeans ignores main().
     *
     * @param args the command line arguments
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws SQLException{
        launch(args);
    }
}