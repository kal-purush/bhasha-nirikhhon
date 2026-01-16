package application;

import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.layout.GridPane;

/**
 * A dialog that takes in data to create new milk data
 */
public class AnnualRepDialog extends Dialog<String> {

	public AnnualRepDialog() {
		this.setTitle("Milk by Year");
		this.setHeaderText("Please input a year for which to display milk total");

		ButtonType displayButtonType = new ButtonType("Display", ButtonData.OK_DONE);
		this.getDialogPane().getButtonTypes().addAll(displayButtonType, ButtonType.CANCEL);

		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));
				
		TextField year = new TextField();
		year.setPromptText("Year");

		grid.add(new Label("Year:"), 0, 0);
		grid.add(year, 1, 0);

		Node loginButton = this.getDialogPane().lookupButton(displayButtonType);
		// can change to true to add reqs to inserting
		loginButton.setDisable(false);

		this.getDialogPane().setContent(grid);

		// set the dialog to return year after submitting valid data
		this.setResultConverter(dialogButton -> {
			if (dialogButton == displayButtonType) {
				try {
					// check for invalid date
					if (Integer.parseInt(year.getText()) > 0) {
						return year.getText();
					}
				}
				catch (NumberFormatException e) {
					// don't allow invalid number fields
					// TODO (possibly) add field validation for dialogs
				}
			}
			return null;
		});
	}


}