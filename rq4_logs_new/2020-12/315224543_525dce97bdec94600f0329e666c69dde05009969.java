package application.controller;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

public class AddListController extends PalController {

	@FXML
	private Label errorLabel;
	@FXML
	private TextField listNameTextField;
	@FXML
	private DatePicker listDatePicker;
	@FXML
	private Button addBtn;
	
	@FXML
	private void addList(final ActionEvent e) {
		System.out.println("Add List");
		System.out.println(listDatePicker.getValue());
		if(listDatePicker.getValue() == null || listNameTextField.getText().equals("")) {
			errorLabel.setVisible(true);
		}
		else {
			errorLabel.setVisible(false);
		}
	}
}