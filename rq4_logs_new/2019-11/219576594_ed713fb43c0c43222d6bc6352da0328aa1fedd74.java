import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.image.Image;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

public class Main extends Application {
	// The dimensions of the window
	private static final int WINDOW_WIDTH = 600;
	private static final int WINDOW_HEIGHT = 400;

	// The dimensions of the canvas
	private static final int CANVAS_WIDTH = 400;
	private static final int CANVAS_HEIGHT = 400;

	// The size of each cell
	private static int GRID_CELL_WIDTH = 25;
	private static int GRID_CELL_HEIGHT = 25;
	
	// The canvas in the GUI. This needs to be a global variable
	// (in this setup) as we need to access it in different methods.
	// We could use FXML to place code in the controller instead.
	private Canvas canvas;
		
	// Loaded images
	Image player;
	Image dirt;
	
	// X and Y coordinate of player
	int playerX = 0;
	int playerY = 0;
	
	public void start(Stage primaryStage) {
		// Build the GUI 
		Pane root = buildGUI();
		
		// Create a scene from the GUI
		Scene scene = new Scene(root, WINDOW_WIDTH, WINDOW_HEIGHT);
		
		// Load images
		player = new Image("Idle.png", 100, 100, false, false);
		dirt = new Image("dirt.png");
		
		// Register an event handler for key presses
		scene.addEventFilter(KeyEvent.KEY_PRESSED, event -> processKeyEvent(event));
		
		// Display the scene on the stage
		drawGame();
		primaryStage.setScene(scene);
		primaryStage.show();		
	}

	public void processKeyEvent(KeyEvent event) {
		switch (event.getCode()) {
			
		    case RIGHT:
		    	// Right key was pressed. So move the player right by one cell.
	        	playerX = playerX + 1;
	        	break;	
		    case LEFT:
		    	// Right key was pressed. So move the player right by one cell.
	        	playerX = playerX - 1;
	        	break;	
		    case UP:
		    	// Right key was pressed. So move the player right by one cell.
	        	playerY = playerY - 1;
	        	break;	
		    case DOWN:
		    	// Right key was pressed. So move the player right by one cell.
	        	playerY = playerY + 1;
	        	break;	
	        default:
	        	// Do nothing
	        	break;
		}
		
		// Redraw game as the player may have moved.
		drawGame();
		
		// Consume the event. This means we mark it as dealt with. This stops other GUI nodes (buttons etc) responding to it.
		event.consume();
	}

	public void drawGame() {
		// Get the Graphic Context of the canvas. This is what we draw on.
		GraphicsContext gc = canvas.getGraphicsContext2D();
		
		// Clear canvas
		gc.clearRect(0, 0, canvas.getWidth(), canvas.getHeight());
		
		// Draw row of dirt images
		// We multiply by the cell width and height to turn a coordinate in our grid into a pixel coordinate.
		// We draw the row at y value 2.
		for (int x = 0; x < CANVAS_WIDTH; x++) {
			gc.drawImage(dirt, x * GRID_CELL_WIDTH,0);	
		}
		for (int x = 0; x < CANVAS_HEIGHT; x++) {
			gc.drawImage(dirt, 0, x * GRID_CELL_HEIGHT);	
		}
		for (int x = 0; x < CANVAS_WIDTH; x++) {
			gc.drawImage(dirt, x * GRID_CELL_WIDTH,x * GRID_CELL_HEIGHT);	
		}
		
		// Draw player at current location
		gc.drawImage(player, playerX * GRID_CELL_WIDTH, playerY * GRID_CELL_HEIGHT);			
	}
	
	public void restartGame() {
		// We just move the player to cell (0, 0) 
		playerX = 0;
		playerY = 0;
		drawGame();
	}
	
	private Pane buildGUI() {
		// Create top-level panel that will hold all GUI
		BorderPane root = new BorderPane();
				
		// Create canvas
		canvas = new Canvas(CANVAS_WIDTH, CANVAS_HEIGHT);
		root.setCenter(canvas);
		
		// Create a toolbar with some nice padding and spacing
		HBox toolbar = new HBox();
		toolbar.setSpacing(10);
		toolbar.setPadding(new Insets(10, 10, 10, 10)); 
		root.setTop(toolbar);
		
		// Create toolbar content
		Button restartButton = new Button("Restart");
		toolbar.getChildren().add(restartButton);

		// Add button event handlers
		restartButton.setOnAction(e -> {
			restartGame();
		});
		
		return root;
	}
	
	public static void main(String[] args) {
		launch(args);
	}
}