package com.groupthree.mancala;

import com.groupthree.mancala.models.Player;
import com.groupthree.mancala.models.PublicInfo;
import com.groupthree.mancala.repository.UserRepository;
import com.groupthree.mancala.util.ApplicationContextManager;
import com.groupthree.mancala.util.LeaderboardProfileCell;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.embed.swing.SwingFXUtils;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.SnapshotParameters;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.AnchorPane;

import javax.imageio.ImageIO;

import java.io.File;
import java.io.IOException;

public class LeaderboardController {

    @FXML
    Button backBtn;

    @FXML
    Button shareButton;

    @FXML
    ListView<Player> leaderboardContainer;

    @FXML
    AnchorPane root;

    private String playerUsername;


    public void initializeScreen(String username) {
        this.playerUsername = username;
        var loggedInPlayer = UserRepository.getInstance().getPlayer(username);
        ObservableList<Player> rankings = FXCollections.observableArrayList();
        var data = UserRepository.getInstance().getPlayerRankings();
        System.out.println("data is " + data);
        rankings.addAll(data);
        leaderboardContainer.setItems(rankings);
        leaderboardContainer.setCellFactory(param -> new LeaderboardProfileCell(loggedInPlayer));
    }


    @FXML
    private void goBack() {
        try {
            var context = ApplicationContextManager.getInstance();
            var previousStage = context.getPreviousStage();
            var previousView = context.getPreviousView();
            FXMLLoader loader = new FXMLLoader(getClass().getResource(previousView));
            Parent root = loader.load();
            PlayerDashboardController controller = loader.getController();
            controller.setWelcomeText(playerUsername);
            Scene scene = new Scene(root);
            previousStage.setScene(scene);
            previousStage.show();
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
    }

    @FXML
    private void takeScreenShot() {
        WritableImage image = root.snapshot(new SnapshotParameters(), null);
        File file = new File("screenshot.png");
        try {
            ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}