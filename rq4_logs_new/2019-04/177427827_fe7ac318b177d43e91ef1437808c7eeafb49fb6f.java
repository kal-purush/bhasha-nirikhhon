import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;

public class HelpScreen {
	public static void display(Stage primaryStage) {
		   // Help Scene
        // set up background border pane (Top/Left/Right/Center/Bottom)
        BorderPane helpBorder = new BorderPane();

        // Screen Size
        int helpWidth = 1000;
        int helpHeight = 700;

        // Background Image--------------
        StackPane helpBackgroundImgContainer = new StackPane();
        ImageView helpBgImage = new ImageView("torch.GIF");//background image
        helpBgImage.setFitHeight(helpHeight);
        helpBgImage.setFitWidth(helpWidth);
        helpBackgroundImgContainer.getChildren().addAll(helpBgImage, helpBorder);
        Config.help = new Scene(helpBackgroundImgContainer, helpWidth, helpHeight);

        // TOP (Menu Button and Title of Help Scene)------------
        HBox hboxTOPHelp = new HBox();
        hboxTOPHelp.setAlignment(Pos.TOP_LEFT);
        // set up for HBox containing the title and the button: (Top/Left/Right/Center/Bottom)
        hboxTOPHelp.setPadding(new Insets(15, 0, 15, 5));
        Button buttonBackToMenu = new Button("Menu");
        buttonBackToMenu.resize(50, 50);
        buttonBackToMenu.setStyle("-fx-background-color: #FFFFFF");
        buttonBackToMenu.setOnMouseEntered(highlightOnMenu -> { //  
            // highlight
            buttonBackToMenu.setStyle("-fx-text-fill: #FFFFFF");
            buttonBackToMenu.setStyle("-fx-border-color: red");
        });
        buttonBackToMenu.setOnMouseExited(highlightOff -> {
            buttonBackToMenu.setStyle("-fx-background-color: #FFFFFF");
        });
        Text helpTitle = new Text("Help");
        helpTitle.setFont(new Font(Config.textFont, 80));//setting font and its size
        helpTitle.setFill(Color.WHITESMOKE);
        helpTitle.setStroke(Color.DARKGOLDENROD);
        buttonBackToMenu.setOnAction(backToMenu -> primaryStage.setScene(Config.menu));//click button and go back to menu screen
        /*
                Text t = new Text(10,50, "Test");
                t.setText("TEST");
                t.setFont(Font.font("Verdana",20));
                t.setFill(Color.WHITESMOKE);
         */
        VBox layout2 = new VBox(20);
        layout2.getChildren().addAll(helpTitle, buttonBackToMenu);
        hboxTOPHelp.setSpacing(295);
        helpBorder.setTop(layout2);

        //end of Help scene
	}
}