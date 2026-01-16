package application;
	
import javafx.application.Application;
import javafx.stage.Stage;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;


public class Main extends Application {
	
	private static final int WINDOW_HEIGHT = 400;
	private static final int WINDOW_WIDTH = 550;
	
	@Override
	public void start(Stage primaryStage) {
//		try {
//			BorderPane root = new BorderPane();
//			Scene scene = new Scene(root,400,400);
//			scene.getStylesheets().add(getClass().getResource("application.css").toExternalForm());
//			primaryStage.setScene(scene);
//			primaryStage.show();
//		} catch(Exception e) {
//			e.printStackTrace();
//		}
		
		Label hello = new Label("Ok, looks like javafx is all set up here");
		BorderPane root = new BorderPane();
		root.setCenter(hello);
		
		Scene mainScene = new Scene(root, WINDOW_WIDTH, WINDOW_HEIGHT);
		primaryStage.setScene(mainScene);
		
		primaryStage.show();
	}
	
	public static void main(String[] args) {
		launch(args);
	}
}