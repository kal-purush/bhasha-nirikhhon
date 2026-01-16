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
import model.Journal;

public class EditJournalPane extends BasePane {

	private model.DataStore db;
	
	private TextField updateJournalName;
	private PasswordField updateJournalPassword;
	
	private GridPane container;
	
	private Journal journal;
	
	public EditJournalPane(Stage stage, Journal journal) {
		super(stage, "Edit Journal Form");
		
		this.db = DataStore.load();
		this.journal = journal;
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		container = new GridPane();
		container.setAlignment(Pos.CENTER);
		container.setHgap(10);
		container.setVgap(10);
		container.setPadding(new Insets(25, 25, 25, 25));

		
		Label name = new Label("Journal Name:");
		container.add(name, 0, 1);
		
		updateJournalName = new TextField(this.journal.name);
		container.add(updateJournalName, 1, 1);
		
		
//		Label password = new Label("Password:");
	//	container.add(password, 0, 2);
		
//		updateJournalPassword = new PasswordField();
//		container.add(updateJournalPassword, 1, 2);
		
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> updateJournal());
		container.add(submit, 0, 3, 1, 2);
		
		
		this.setCenter(container);
	}
	
	private void updateJournal() {
		//TODO: Error handling for the form
		try {
			String name = updateJournalName.getText();
			String password = updateJournalPassword.getText();
			
			
			this.journal.name = name;
			//this.journal.setPassword(password);
		
			//this.db.university.Journals.add(Journal);
			this.db.serialize();
			
			Navigation.navigate(AdministratorPane.class);
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
}
