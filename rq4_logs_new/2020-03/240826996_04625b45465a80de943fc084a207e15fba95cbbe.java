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
import model.Researcher;
import model.Reviewer;

public class EditResearcherPane extends BasePane {

	private model.DataStore db;
	
	private TextField updateResearcherName;
	private PasswordField updateResearcherPassword;
	
	private GridPane container;
	
	private Researcher researcher;
	
	public EditResearcherPane(Stage stage, Researcher researcher) {
		super(stage, "Edit Researcher Form");
		
		this.db = DataStore.load();
		this.researcher = researcher;
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		container = new GridPane();
		container.setAlignment(Pos.CENTER);
		container.setHgap(10);
		container.setVgap(10);
		container.setPadding(new Insets(25, 25, 25, 25));

		
		Label name = new Label("Researcher Name:");
		container.add(name, 0, 1);
		
		updateResearcherName = new TextField(this.researcher.name);
		container.add(updateResearcherName, 1, 1);
		
		
		Label password = new Label("Password:");
		container.add(password, 0, 2);
		
		updateResearcherPassword = new PasswordField();
		container.add(updateResearcherPassword, 1, 2);
		
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> updateResearcher());
		container.add(submit, 0, 3, 1, 2);
		
		
		this.setCenter(container);
	}
	
	private void updateResearcher() {
		//TODO: Error handling for the form
		try {
			String name = updateResearcherName.getText();
			String password = updateResearcherPassword.getText();
			
			
			this.researcher.name = name;
			this.researcher.setPassword(password);
		
			this.db.serialize();
			
			Navigation.navigate(AdministratorPane.class);
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
}
