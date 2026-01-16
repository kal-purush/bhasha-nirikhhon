package application;

import java.io.IOException;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

public class HomeScreenController {

    @FXML
    private Button trickButton;

    @FXML
    private Button treatButton;
    
    @FXML
    private Label trickCount;
    
    @FXML
    private Label treatCount;
	
    @FXML
    private TextField qtyField;
    
    @FXML
    private AnchorPane trickPane;
    
    private int numTricks = 0;
    private int numTreats = 0;
    
	@FXML
	public void addOneTrick(ActionEvent e) throws IOException {
		numTricks += Integer.valueOf(qtyField.getText());
		trickCount.setText("Count: "+Integer.toString(numTricks));
		if(numTricks > 20) {
			switchTrickScene(e);
		}
	}
	
	@FXML
	public void addOneTreat(ActionEvent e) {
		numTreats += Integer.valueOf(qtyField.getText());
		treatCount.setText("Count: "+Integer.toString(numTreats));
	}
	
	public void switchTrickScene(ActionEvent event) throws IOException {
		trickPane = FXMLLoader.load(getClass().getResource("TrickScene.fxml"));
		Scene scene = new Scene(trickPane);// pane you are GOING TO show
    	scene.getStylesheets().add(getClass().getResource("application.css").toExternalForm());
        Stage window = (Stage) ((Node)event.getSource()).getScene().getWindow();// pane you are ON
        window.setScene(scene);
        window.show();
	}
}