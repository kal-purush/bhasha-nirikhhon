import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;

public class Main extends Application {
	
	private static final int MENU_WINDOW_WIDTH = 600;
	private static final int MENU_WINDOW_HEIGHT = 400;
	public static final String WINDOW_TITLE = "Chips Challenge";

	public static final int GAME_WINDOW_WIDTH = 600;
	public static final int GAME_WINDOW_HEIGHT = 600;
	
	public static final int CREATE_USER_WINDOW_WIDTH = 400;
	public static final int CREATE_USER_WINDOW_HEIGHT = 150;
	
	public void start(Stage primaryStage) {
		try {
			// Load the main scene.
			GridPane root = (GridPane)FXMLLoader.load(getClass().getResource("Menu.fxml"));
			Scene scene = new Scene(root, MENU_WINDOW_WIDTH, MENU_WINDOW_HEIGHT);

			// Place the main scene on stage and show it.
			primaryStage.setScene(scene);
			primaryStage.setTitle(WINDOW_TITLE);
			primaryStage.show();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		launch(args);
	}

}