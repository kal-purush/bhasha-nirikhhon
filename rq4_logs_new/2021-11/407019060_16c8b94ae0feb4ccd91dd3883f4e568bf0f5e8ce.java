import javafx.application.Application;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.stage.Stage;

public class CheckBoxDemo_Program_Control extends Application {
	
	CheckBox keyboardCheckBox = new CheckBox("Keyboard");
	CheckBox mouseCheckBox = new CheckBox("Mouse");
	CheckBox touchScreenCheckBox = new CheckBox("Touch Screen");
	
	Label headingLabel = new Label("Select Input Devices:");
	Label responseLabel = new Label("You haven't selected any device.");
	Label selectedLabel = new Label("Supported devices: ");
	
	String inputDeviceString = "";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		// TODO Auto-generated method stub
		
	}

}