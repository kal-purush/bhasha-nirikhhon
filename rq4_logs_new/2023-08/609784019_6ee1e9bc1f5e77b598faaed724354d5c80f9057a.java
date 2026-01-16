package application.viewControllers;
import java.io.IOException;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.stage.Stage;

public class ActivityCreationController {
	
	 @FXML
	 private TextField clientGrabberCreator;

	 @FXML
	 private TextField doctorGrabberCreator;

	 @FXML
	 private TextField patientIdGrabberCreator ;
	 
	 @FXML
	 private TextField timeGrabberCreator;
	 
	 @FXML	
	 public void handleCreateNewActivityAction(){
		 
	 }
	 
	
	 @FXML	
	 public void switchToHomepage(MouseEvent mouseEvent) throws IOException{
		 Parent root = FXMLLoader.load(getClass().getResource("/application/fxmlscenes/calendar_new.fxml"));
		 Stage stage = (Stage)((Node)mouseEvent.getSource()).getScene().getWindow();
		 Scene scene = new Scene(root);
		 stage.setScene(scene);
		 stage.show();
	 }

}