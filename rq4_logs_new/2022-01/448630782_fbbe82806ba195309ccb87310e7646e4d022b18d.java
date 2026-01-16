package net.htlgkr.groupK.chess;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class Main extends Application{
    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(Main.class.getResource("menu-screen.fxml"));
        Scene scene = new Scene(fxmlLoader.load(), 500, 700);
        stage.setResizable(false);
        stage.setTitle("Schach");
        stage.setScene(scene);
        stage.show();


        UI_LoginPrompt_Controller cnt = fxmlLoader.getController();

        Server server = new Server(cnt);
        server.startListening();

        cnt.getIdJoinGameButton().setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent actionEvent) {
                Client client = new Client(cnt);
                client.startClient();
            }
        });
    }

    public static void main(String[] args) {
        launch();
    }
}