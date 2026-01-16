package view;

import global.Navigation;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import model.DataStore;
import model.Editor;
import model.Journal;
import model.Researcher;
import model.Reviewer;

public class NewJournalPane extends BasePane {

	private model.DataStore db;
	
	private TextField newJournalName;
	private ComboBox<Editor> selectedEditor;
	
	private GridPane container;
	
	public NewJournalPane(Stage stage) {
		super(stage, "New Journal Form");
		
		this.db = DataStore.load();
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		container = new GridPane();
		container.setAlignment(Pos.CENTER);
		container.setHgap(10);
		container.setVgap(10);
		container.setPadding(new Insets(25, 25, 25, 25));

		
		Label name = new Label("Journal Name:");
		container.add(name, 0, 1);
		
		newJournalName = new TextField();
		container.add(newJournalName, 1, 1);
		
		try {
			selectedEditor = new ComboBox<Editor>();
			selectedEditor.getItems().addAll(this.db.university.editors);
			container.add(selectedEditor, 1, 2);
		} catch (Exception e) {
			// TODO: Deal with this properly
		}
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> createNewJournal());
		container.add(submit, 0, 3, 1, 2);
		
		
		this.setCenter(container);
	}
	
	private void createNewJournal() {
		//TODO: Error handling for the form
		try {
			String name = newJournalName.getText();
			Editor editor = selectedEditor.getValue();
			
			
			
			Journal journal = new Journal(name);
			journal.editor = editor;
		
			this.db.university.journals.add(journal);
			this.db.serialize();
			
			Navigation.navigate(JournalListPane.class);
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
}