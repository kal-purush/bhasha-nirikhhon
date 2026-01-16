import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Label;
import javafx.scene.image.ImageView;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;

public class GameController {

    @FXML private GridPane gridPane;
    @FXML private ImageView Image;
    @FXML private Canvas canvas;
    @FXML private Label projectLevel;
    @FXML private Label tokenCount;
    @FXML private Label redCount;
    @FXML private Label yellowCount;
    @FXML private Label blueCount;

    public void initialize() {
    	projectLevel.setText("Level - 1");
    	gridPane.addEventFilter(KeyEvent.KEY_PRESSED, event -> keyPressed(event));
    	drawGame();
    }
    @FXML 
    void clickQuit(ActionEvent event) {
    	System.exit(0);
    }

    @FXML
    void clickRestart(ActionEvent event) {
    	Game.playerX = 0;
    	Game.playerY = 0;
    	drawGame();
    }

    @FXML
    void clickSave(ActionEvent event) {

    }

    @FXML
    void keyPressed(KeyEvent event) {
    	switch (event.getCode()) {
		
	    case RIGHT:
	    	// Right key was pressed. So move the player right by one cell.
        	Game.playerX = Game.playerX + 1;
        	break;	
	    case LEFT:
	    	// Right key was pressed. So move the player right by one cell.
        	Game.playerX = Game.playerX - 1;
        	break;	
	    case UP:
	    	// Right key was pressed. So move the player right by one cell.
        	Game.playerY = Game.playerY - 1;
        	break;	
	    case DOWN:
	    	// Right key was pressed. So move the player right by one cell.
        	Game.playerY = Game.playerY + 1;
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
		
		gc.setStroke(Color.BLACK);
		gc.strokeRect(0, 0, Game.CANVAS_WIDTH, Game.CANVAS_HEIGHT);

		for(int k = 0; k < Game.height; k++) {
			
			for(int i = 0; i < Game.width; i++) {
				
				String instance = Game.map.get((k*Game.width)+i);
				
                if (instance.equals("Wall")) {
                	gc.setStroke(Color.BLACK);
                	gc.strokeRect((i - Game.playerX + 3) * Game.GRID_CELL_WIDTH, (k - Game.playerY + 3) * Game.GRID_CELL_HEIGHT, Game.GRID_CELL_WIDTH, Game.GRID_CELL_HEIGHT);
                }
                else if (instance.equals("Ground")) {
                	gc.setFill(Color.WHITE);
                	gc.fillRect((i - Game.playerX + 3) * Game.GRID_CELL_WIDTH, (k - Game.playerY + 3) * Game.GRID_CELL_HEIGHT, Game.GRID_CELL_WIDTH, Game.GRID_CELL_HEIGHT);
                }
                else if (instance.equals("Goal")) {    
                	gc.setFill(Color.GREEN);
                	gc.fillRect((i - Game.playerX + 3) * Game.GRID_CELL_WIDTH, (k - Game.playerY + 3) * Game.GRID_CELL_HEIGHT, Game.GRID_CELL_WIDTH, Game.GRID_CELL_HEIGHT);
                }
                else {
                    System.out.println("Error: instance not found.");
                }
			}
		}
		// Draw player at centre cell
		gc.drawImage(Game.player, 3 * Game.GRID_CELL_WIDTH, 3 * Game.GRID_CELL_HEIGHT);			
	}		

}