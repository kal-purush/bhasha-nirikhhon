package application.controller;

import java.net.URL;
import java.util.ResourceBundle;

import application.model.ShowData;
import application.model.Users;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;

public class AddUserController implements Initializable, SubController{
	MainController mc;
	Users user;
	
	//FXML TEXT Fields
	
	@FXML
	private Button SubmitNewAccount;
	
	
	
	
	
	//FXML Event Hanlder for validating fields and adding user
	public void addUser(ActionEvent event) {
		mc.updateView(null, MainController.loginScreenView, MainController.loginScreenX, MainController.loginScreenY);
	}
	

	@Override
	public void onLoad(ShowData data, MainController mc) {
		// TODO Auto-generated method stub
		this.mc = mc;
	}

	@Override
	public ShowData onExit() {
		// TODO Auto-generated method stub
		return user;
	}

	@Override
	public void initialize(URL location, ResourceBundle resources) {
		// TODO Auto-generated method stub
		
	}

}