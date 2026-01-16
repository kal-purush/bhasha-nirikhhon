package tictactoe.alert;

import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import java.util.Optional;

public class IncomingRequestDialog {

    public Optional<Boolean> showRequestDialog(String opponentName, String score) {
      
        Dialog<Boolean> dialog = new Dialog<>();
        dialog.setTitle("Game Challenge");
        dialog.setHeaderText(null); 

     
        dialog.getDialogPane().getStyleClass().add("dialog-pane");
    
        Label titleLabel = new Label("You have received a game challenge!");
        titleLabel.getStyleClass().add("dialog-title");

        ImageView avatarImage = new ImageView(this.getClass().getResource("/resources/images/avatar.png").toString());
        avatarImage.setFitWidth(60);
        avatarImage.setFitHeight(60);
        avatarImage.getStyleClass().add("avatar-image");

        Label opponentLabel = new Label("userName: " + opponentName);
        opponentLabel.getStyleClass().add("opponent-label");

        Label scoreLabel = new Label("score: " + score);
        scoreLabel.getStyleClass().add("score-label");

        VBox labelsContainer = new VBox(3, opponentLabel, scoreLabel);
        labelsContainer.setAlignment(Pos.CENTER_LEFT);

        HBox headerContainer = new HBox(10, avatarImage, labelsContainer);
        headerContainer.setAlignment(Pos.CENTER_LEFT);

        Label messageLabel = new Label(
                "You have been invited to join an exciting game.\nWill you accept the challenge or decline it?");
        messageLabel.getStyleClass().add("message-label");
        messageLabel.setWrapText(true);
        messageLabel.setMaxWidth(400);

        ButtonType acceptButtonType = new ButtonType("Accept", ButtonBar.ButtonData.OK_DONE);
        ButtonType declineButtonType = new ButtonType("Decline", ButtonBar.ButtonData.CANCEL_CLOSE);

        dialog.getDialogPane().getButtonTypes().addAll(acceptButtonType, declineButtonType);

        Button acceptButton = (Button) dialog.getDialogPane().lookupButton(acceptButtonType);
        acceptButton.getStyleClass().add("accept-button");

        Button declineButton = (Button) dialog.getDialogPane().lookupButton(declineButtonType);
        declineButton.getStyleClass().add("decline-button");

        HBox buttonContainer = new HBox(10, acceptButton, declineButton);
        buttonContainer.setAlignment(Pos.CENTER);

        VBox contentPane = new VBox(15, titleLabel, headerContainer, messageLabel, buttonContainer);
        contentPane.getStyleClass().add("content-pane");

        dialog.getDialogPane().setContent(contentPane);

        // Handle the result
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == acceptButtonType) {
                return true;  // Accept button clicked
            } else {
                return false; // Decline button clicked
            }
        });

        //dialog.getDialogPane().getStylesheets().add(this.getClass().getResource("/CSS/homeStyle.css").toExternalForm());

        return dialog.showAndWait();
    }
}