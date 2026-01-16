package tictactoe.ui.alert;

import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.stage.Stage;

public class EndReplayGameAlert {

    public Scene showAlert(Stage mainStage, Stage alertStage, Runnable onExitAction, char winner, String winnerName) {
        BorderPane root = new BorderPane();
        root.setStyle("-fx-background-color: #1E5AAF; -fx-border-radius: 10px; -fx-background-radius: 10px;");

        Font font = Font.loadFont(getClass().getResourceAsStream("/resources/fonts/MyCustomFont.ttf"), 24.0);

        String message;
        
        switch (winner) {
            case 'W':
                message = "Winner";
                break;
            case 'L':
                message = "Winner";
                break;
            default:
                message = "Draw";
                winnerName = "No Winner";
                break;
        }

        Label messageLabel = new Label(message);
        messageLabel.setFont(font);
        messageLabel.setTextFill(Color.YELLOW);

        Label winnerLabel = new Label(winnerName);
        winnerLabel.setFont(font);
        winnerLabel.setTextFill(Color.WHITE);

        
        Button exitBtn = new Button("Exit");
        exitBtn.setFont(font);
        exitBtn.setStyle("-fx-background-color: #DC3545; -fx-text-fill: white; -fx-padding: 10 20; -fx-background-radius: 5;");
        exitBtn.setOnAction(e -> {
            alertStage.close();  
            onExitAction.run();  
        });

        VBox contentBox = new VBox(messageLabel, winnerLabel, exitBtn);
        contentBox.setSpacing(20); 
        contentBox.setAlignment(Pos.CENTER); 

        
        root.setCenter(contentBox);

        return new Scene(root, 300, 250);
    }
}