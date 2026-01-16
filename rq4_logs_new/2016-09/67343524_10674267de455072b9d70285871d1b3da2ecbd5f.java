package gamelib.scenes;

import gamelib.GameManager;
import gamelib.game.Level;
import processing.core.PConstants;
import processing.core.PGraphics;

/**
 * This scene contains all the game logic and entities.
 *
 * @author Rebecca Stevens
 */
public class GameScene extends Scene {

	/**
	 * The game's graphic object.
	 */
	private PGraphics gameGraphics;
	
	/**
	 * The active level.
	 */
	private Level level;
	
	/**
	 * Create the game scene.
	 */
	public GameScene() {
		createGameGraphics();
	}

	@Override
	public void enter() {
	}

	@Override
	public void leave() {
	}

	@Override
	public void update(float delta) {
		if (this.level != null) {
			this.level.update(delta);
		}
	}

	@Override
	public void draw(PGraphics g) {
		g.background(0);	// create black bars around the game if needed. 
		if (this.level != null) {
			
			this.gameGraphics.beginDraw();
			this.level.drawBackground(this.gameGraphics);
			this.level.draw(this.gameGraphics);
			this.level.drawOverlay(this.gameGraphics);
//			this.level.drawGrid(this.gameGraphics);
			this.gameGraphics.endDraw();
			
			g.imageMode(PConstants.CORNER);
			g.image(gameGraphics, (g.width - this.gameGraphics.width) / 2, (g.height - this.gameGraphics.height) / 2);
		}
	}
	
	/**
	 * Set the active level.
	 * 
	 * @return
	 */
	public Level getActiveLevel() {
		return level;
	}

	/**
	 * Get the currently active level.
	 * 
	 * @param level - The level to make active
	 */
	public void setActiveLevel(Level level) {
		this.level = level;
	}

	/**
	 * The width of the game viewport (in pixels).
	 * 
	 * @return
	 */
	public int getGameWidth() {
		return this.gameGraphics.width;
	}

	/**
	 * The height of the game viewport (in pixels).
	 * 
	 * @return
	 */
	public int getGameHeight() {
		return this.gameGraphics.height;
	}

	/**
	 * Create the game graphics.
	 */
	private void createGameGraphics() {
		GameManager gm = GameManager.getMe();
		
		int windowWidth = gm.getGraphics().width;
		int windowHeight = gm.getGraphics().height;
		int gameWidth;
		int gameHeight;
		
//		if (((float)windowWidth) / windowHeight <= aspectRatio) {
//			gameWidth = windowWidth;
//			gameHeight = (int) (windowWidth / aspectRatio);
//		} else {
//			gameWidth = (int) (windowHeight * aspectRatio);
//			gameHeight = windowHeight;
//		}
		gameWidth = windowWidth;
		gameHeight = windowHeight;
	
		this.gameGraphics = gm.getSketch().createGraphics(gameWidth, gameHeight);
	}

}