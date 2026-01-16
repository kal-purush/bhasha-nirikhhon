package application.controller;

import application.CommonObjs;
import data_access_layer.DataAccessLayer;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

public class ManageAssetController
{
	private CommonObjs commonObjs = CommonObjs.getInstance();
	@FXML private TextField search_term;
	@FXML private Label result_message;
	private DataAccessLayer DAL = new DataAccessLayer();
	
	@FXML public void searchAssetOp() 
	{	
		System.out.println("Testing");
	}
}