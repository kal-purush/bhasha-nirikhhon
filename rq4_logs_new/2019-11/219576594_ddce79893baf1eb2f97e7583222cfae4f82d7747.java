import javafx.scene.canvas.GraphicsContext;
import javafx.scene.image.Image;

public class Token extends Entity {
	
	protected int size = 40;
	protected Image sprite = new Image("Token.png", size, size, false, false);
	protected boolean collected = false;

	public Token (int x, int y) {
		super (x, y);
	}
	
	public void draw (GraphicsContext g) {
		if (collected == false) {
			g.drawImage(sprite, (x - GameController.playerX + 3) * GameController.getGridCellWidth()
					              + (GameController.getGridCellWidth() - size)/2,
					            (y - GameController.playerY + 3) * GameController.getGridCellHeight()
					              + (GameController.getGridCellHeight() - size)/2);
			// The "+" part is to centre the sprite in the middle of the cell
		}
	}
	
	public void checkIfTouched (GraphicsContext g) {
		if ((GameController.playerX == x) && (GameController.playerY == y)) {
			collected = true;
			//Setting to a null location
			x = -100;
			y = -100;
			//Redraw cell and player to remove token image
			g.fillRect(GameController.getGridCellWidth() * 3,  GameController.getGridCellHeight() * 3, GameController.getGridCellWidth(), GameController.getGridCellHeight());
			g.drawImage(GameController.player, 3 * GameController.getGridCellWidth(), 3 * GameController.getGridCellHeight());
			System.out.println("Token Collected");
		}
	}
	
}