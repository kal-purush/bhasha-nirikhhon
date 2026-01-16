package global;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.RuntimeException;

import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

/**
 * A global navigation object used to navigate between "pages"
 *
 */
public class Navigation {

	public static Stage primaryStage;
	
	public static void setPrimaryStage(Stage stage) {
		Navigation.primaryStage = stage;
	}
	
	
	public static void navigate(Pane newPane) {
		Scene scene = generateSceneForPane(newPane);
		primaryStage.setScene(scene);
	}
	
	public static void navigate(Class<? extends Pane> pane) {
		try {
			Constructor constructor = pane.getConstructor(Stage.class);
			Pane newPane = (Pane) constructor.newInstance(primaryStage);
			Scene newScene = generateSceneForPane(newPane);
			primaryStage.setScene(newScene);
		} catch(Exception e) {
			// TODO Handle errors properly
			e.printStackTrace();	
		}
	}
	
	private static Scene generateSceneForPane(Pane pane) {
		Scene scene = new Scene(pane, 600, 600);
        return scene;
	}
}