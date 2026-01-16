package view;

import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

public class LoginPane extends BasePane {

	public LoginPane(Stage primaryStage) {
		super(primaryStage, "Login Page");
		
        TextField login_tf = new TextField();

        BorderPane container = new BorderPane();
        
        Button login_b = new Button("Press Me");
        // TODO: set this to have logic based on the account given as input
        login_b.setOnAction(e -> {
            //primaryStage.setScene(researcher_scene);
        });

        container.setTop(login_b);
        container.setCenter(login_tf);
        this.setCenter(container);
        
        //login_scene = new Scene(login_layout, 800, 600);
	}
}