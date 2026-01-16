package net.htlgkr.groupK.chess;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

public class CNT_passwordIncorrectScreenClient implements Initializable {
    @FXML
    public Button btn_enterDataAgain;

    public Button getBtn_enterDataAgain() {
        return btn_enterDataAgain;
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        this.btn_enterDataAgain.setOnAction(actionEvent -> {
            try {
                System.out.println("open new loginPrompt");
                Main.createLoginPrompt(Main.stage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}