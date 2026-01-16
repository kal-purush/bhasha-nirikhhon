package view;

import java.util.ArrayList;

import javafx.scene.control.Label;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.CornerRadii;

import javafx.scene.paint.Color;
import javafx.stage.Stage;
import javafx.geometry.Insets;

public class JournalListPane extends BorderPane {

	private ArrayList<model.Journal> createFakeJournals() {
		ArrayList<model.Journal> fake = new ArrayList<model.Journal>();
		fake.add(new model.Journal("a fake journal"));
		fake.add(new model.Journal("another fake journal"));
		fake.add(new model.Journal("a third fake journal"));
		fake.add(new model.Journal("a final fake journal"));
		
		return fake;
	}
	
	public JournalListPane(Stage stage) {
		
		ArrayList<model.Journal> journals = createFakeJournals();
		
		Label titleLabel = new Label("Journal List Page");
		
		Pane topPane = new Pane();
		topPane.getChildren().addAll(titleLabel);
		
		topPane.setBackground(new Background(Color.RED, CornerRadii.EMPTY, Insets.EMPTY));
		
		this.setTop(topPane);
	}
}