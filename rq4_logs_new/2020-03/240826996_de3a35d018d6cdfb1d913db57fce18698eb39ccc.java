package view;

import java.util.ArrayList;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import model.DataStore;
import model.Reviewer;

public class AdministratorPane extends BasePane {
	
	private model.DataStore db;
	private Stage primaryStage;
	private Pane mainPane;
	
	private TextField newReviewerName;
	private PasswordField newReviewerPassword;
	
	
	public AdministratorPane(Stage stage) {
		super(stage, "Administrator Page");

		this.db = DataStore.load();
		
		this.primaryStage = stage;
		
		this.mainPane = new Pane();
		
		displayReviewers();
		
		this.setCenter(mainPane);
		
	}
	
	private void displayReviewers() {
		mainPane.getChildren().clear();
		this.db = DataStore.load();
		
		
		ArrayList<Reviewer> reviewers = db.university.reviewers;
		
		VBox reviewerContainer = new VBox();
		
		Label header = new Label("List of Reviewers:");
		reviewerContainer.getChildren().add(header);
		
		if(reviewers.size() <= 0) {
			Label message = new Label("There are no reviewers");
			reviewerContainer.getChildren().add(message);
		}
		else {
			for(Reviewer r : reviewers) {
				HBox reviewerListItem = new HBox();
				Label reviewerLabel = new Label(r.name);
				Button viewReviewerButton = new Button("View Reviewer");
				
				reviewerListItem.getChildren().add(reviewerLabel);
				reviewerListItem.getChildren().add(viewReviewerButton);
				
				reviewerContainer.getChildren().add(reviewerListItem);
			}
		}
		
		
		/////////////
		Button newReviewer = new Button("New Reviewer");
		
		newReviewer.setOnAction(event -> displayNewReviewerPane());
		
		reviewerContainer.getChildren().addAll(newReviewer);
		/////////////
		
		
		mainPane.getChildren().add(reviewerContainer);
	}
	
	private void displayNewReviewerPane() {
		mainPane.getChildren().clear();
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		GridPane newReviewerPane = new GridPane();
		newReviewerPane.setAlignment(Pos.CENTER);
		newReviewerPane.setHgap(10);
		newReviewerPane.setVgap(10);
		newReviewerPane.setPadding(new Insets(25, 25, 25, 25));
		
		Label title = new Label("New Reviewer Form");
		newReviewerPane.add(title, 0, 0, 2, 1); // Have the title span 2 columns
		
		
		Label name = new Label("Reviewer Name:");
		newReviewerPane.add(name, 0, 1);
		
		newReviewerName = new TextField();
		newReviewerPane.add(newReviewerName, 1, 1);
		
		
		Label password = new Label("Password:");
		newReviewerPane.add(password, 0, 2);
		
		newReviewerPassword = new PasswordField();
		newReviewerPane.add(newReviewerPassword, 1, 2);
		
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> createNewReviewer());
		newReviewerPane.add(submit, 0, 3, 1, 2);
				
		mainPane.getChildren().add(newReviewerPane);
	}
	
	private void createNewReviewer() {
		//TODO: Error handling for the form
		try {
			String name = newReviewerName.getText();
			String password = newReviewerPassword.getText();
			
			
			Reviewer reviewer = new Reviewer(name, password);
			
			this.db.university.reviewers.add(reviewer);
			this.db.serialize();
			
			displayReviewers();
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
	
	
}