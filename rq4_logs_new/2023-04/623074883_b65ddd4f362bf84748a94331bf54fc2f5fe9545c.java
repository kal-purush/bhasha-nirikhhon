package com.groupthree.mancala;

import com.groupthree.mancala.models.Player;
import javafx.application.Application;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.SelectionMode;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/**
 * version 1.0
 *
 *
 * @author Steve
 */
public class AdminDashboard extends Application {

    ListView<String> playersList;

    Scene scene;

    Stage window;

    //Constructor for use method
    public AdminDashboard(){};
    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {;
        window = stage;
        window.setTitle("Admin Dashboard");

        //Label player list
        Label label = new Label("Player list:");

        //Create player
        Player p1 = new Player("Alice", "Alice", "A",
                "/Users/fengyingcong/Documents/GitHub/Mancala/pictures/profile1.jpeg");
        Player p2 = new Player("Bob", "Bob", "B",
                "/Users/fengyingcong/Documents/GitHub/Mancala/pictures/profile1.jpeg");
        Player p3 = new Player("Cane", "Cane", "C",
                "/Users/fengyingcong/Documents/GitHub/Mancala/pictures/profile1.jpeg");
        Player p4 = new Player("David", "David", "D",
                "/Users/fengyingcong/Documents/GitHub/Mancala/pictures/profile1.jpeg");
        Player p5 = new Player("Elven", "Elven", "E",
                "/Users/fengyingcong/Documents/GitHub/Mancala/pictures/profile1.jpeg");

        //Player list
        playersList = new ListView<>();
        playersList.getItems().addAll(
                p1.getUsername(),
                p2.getUsername(),
                p3.getUsername(),
                p4.getUsername(),
                p5.getUsername()
        );
        playersList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        //Buttons
        Button approveButton = new Button("Approve");
        approveButton.setOnAction(e -> approve());

        Button extraInfoButton = new Button("Extra information");
        extraInfoButton.setOnAction(e -> ExtraInfo.display("Extra information"));


        //VBox layout
        VBox layout = new VBox(10);
        layout.setPadding(new Insets(20));
        layout.getChildren().addAll(label, playersList, approveButton, extraInfoButton);

        //Setting scene
        scene = new Scene(layout, 300, 400);
        window.setScene(scene);
        window.show();

    }

    private void approve() {
        //I don't know how approve work so here is just print some information
        System.out.println("User " + playersList.getSelectionModel().getSelectedItem() + " approved");
        //Approve function code
    }

    private void addPlayerToList(){

    }
}