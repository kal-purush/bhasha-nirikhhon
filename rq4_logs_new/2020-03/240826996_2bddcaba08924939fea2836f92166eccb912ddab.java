package view;

import global.Navigation;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import model.DataStore;
import model.Reviewer;

public class EditReviewerPane extends BasePane {

	private model.DataStore db;
	
	private TextField updateReviewerName;
	private PasswordField updateReviewerPassword;
	
	private GridPane container;
	
	private Reviewer reviewer;
	
	public EditReviewerPane(Stage stage, Reviewer reviewer) {
		super(stage, "Edit Reviewer Form");
		
		this.db = DataStore.load();
		this.reviewer = reviewer;
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		container = new GridPane();
		container.setAlignment(Pos.CENTER);
		container.setHgap(10);
		container.setVgap(10);
		container.setPadding(new Insets(25, 25, 25, 25));

		
		Label name = new Label("Reviewer Name:");
		container.add(name, 0, 1);
		
		updateReviewerName = new TextField(this.reviewer.name);
		container.add(updateReviewerName, 1, 1);
		
		
		Label password = new Label("Password:");
		container.add(password, 0, 2);
		
		updateReviewerPassword = new PasswordField();
		container.add(updateReviewerPassword, 1, 2);
		
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> updateReviewer());
		container.add(submit, 0, 3, 1, 2);
		
		
		this.setCenter(container);
	}
	
	private void updateReviewer() {
		//TODO: Error handling for the form
		try {
			String name = updateReviewerName.getText();
			String password = updateReviewerPassword.getText();
			
			
			this.reviewer.name = name;
			this.reviewer.setPassword(password);
		
			//this.db.university.reviewers.add(reviewer);
			this.db.serialize();
			
			Navigation.navigate(AdministratorPane.class);
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
}
