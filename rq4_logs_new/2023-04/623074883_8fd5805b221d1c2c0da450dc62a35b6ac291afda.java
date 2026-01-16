package com.groupthree.mancala;

import com.groupthree.mancala.models.Admin;
import com.groupthree.mancala.models.Player;
import com.groupthree.mancala.models.PublicInfo;
import com.groupthree.mancala.repository.UserRepository;
import com.groupthree.mancala.util.ApplicationContextManager;
import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.SelectionMode;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.io.IOException;

/**
 * version 1.0
 *
 * @author Steve
 */
public class AdminDashboardController {


    private Stage stage;
    private Scene scene;
    private Parent root;

    private String adminUsername;

    Stage window;
    @FXML
    HBox playerApprovalContainer;


    @FXML
    Label welcomeText;

    @FXML
    public void onProfileButtonClick(ActionEvent event) throws IOException {
        var admin = UserRepository.getInstance().getAdmin(adminUsername);
        System.out.println("on profile clicked ...");
        FXMLLoader loader = new FXMLLoader(getClass().getResource("admin-profile-view.fxml"));
        root = loader.load();
        AdminProfileController profileController = loader.getController();
        profileController.setUp(admin);
        stage = (Stage) ((Node) event.getSource()).getScene().getWindow();
        var context = ApplicationContextManager.getInstance();
        context.addStage(stage);
        context.addView("admin-dashboard-view.fxml");
        scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }

    @FXML
    public void onGameStatsClick() {

    }


    public void initializeScreen(String adminName) {
        Admin admin = UserRepository.getInstance().getAdmin(adminName);
        this.adminUsername = admin.getUsername();
        welcomeText.setText("Welcome " + adminName);
        //Label player list
        Label label = new Label("Players that need to be approved");


        ObservableList<PublicInfo> approvalsList = FXCollections.observableArrayList();
        var data = UserRepository.getInstance().getAllPlayersPendingApproval();
        System.out.println("data is " + data);
        approvalsList.addAll(data);
        ListView<PublicInfo> playersList = new ListView<>(approvalsList);
        playersList.setPrefSize(300, 170);
        playersList.setCellFactory(param -> new PlayerApprovalProfileCell() {
            @Override
            public void updateList() {
                var userProfile = getItem();
                playersList.getItems().remove(userProfile);
            }
        });

        //VBox layout
        VBox layout = new VBox(10);
        layout.setPadding(new Insets(20));
        layout.getChildren().addAll(label, playersList);

        //Setting scene
        playerApprovalContainer.getChildren().add(layout);

    }

    private void approve() {
        //I don't know how approve work so here is just print some information
        System.out.println("User " + " approved");
        //Approve function code
    }

    private void addPlayerToList() {

    }
}