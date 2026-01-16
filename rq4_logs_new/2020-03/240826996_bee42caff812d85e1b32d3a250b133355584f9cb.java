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
import model.Editor;
import model.Researcher;
import model.Reviewer;

public class EditEditorPane extends BasePane {

	private model.DataStore db;
	
	private TextField updateEditorName;
	private PasswordField updateEditorPassword;
	
	private GridPane container;
	
	private Editor editor;
	
	public EditEditorPane(Stage stage, Editor editor) {
		super(stage, "Edit Editor Form");
		
		this.db = DataStore.load();
		this.editor = editor;
		
		//https://docs.oracle.com/javafx/2/get_started/form.htm
		container = new GridPane();
		container.setAlignment(Pos.CENTER);
		container.setHgap(10);
		container.setVgap(10);
		container.setPadding(new Insets(25, 25, 25, 25));

		
		Label name = new Label("Editor Name:");
		container.add(name, 0, 1);
		
		updateEditorName = new TextField(this.editor.name);
		container.add(updateEditorName, 1, 1);
		
		
		Label password = new Label("Password:");
		container.add(password, 0, 2);
		
		updateEditorPassword = new PasswordField();
		container.add(updateEditorPassword, 1, 2);
		
		
		Button submit = new Button("Submit");
		submit.setOnAction(event -> updateEditor());
		container.add(submit, 0, 3, 1, 2);
		
		
		this.setCenter(container);
	}
	
	private void updateEditor() {
		//TODO: Error handling for the form
		try {
			String name = updateEditorName.getText();
			String password = updateEditorPassword.getText();
			
			
			this.editor.name = name;
			this.editor.setPassword(password);
		
			this.db.serialize();
			
			Navigation.navigate(AdministratorPane.class);
		}
		catch(Exception e) {
			// TODO: proper error handling
		}
	}
}
