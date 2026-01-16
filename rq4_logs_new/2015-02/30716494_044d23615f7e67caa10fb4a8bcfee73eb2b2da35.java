package game;

import java.awt.Font;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.lwjgl.input.Mouse;
import org.newdawn.slick.AppGameContainer;
import org.newdawn.slick.BasicGame;
import org.newdawn.slick.Color;
import org.newdawn.slick.GameContainer;
import org.newdawn.slick.SlickException;
import org.newdawn.slick.TrueTypeFont;
import org.newdawn.slick.geom.MorphShape;
import org.newdawn.slick.geom.Polygon;
import org.newdawn.slick.geom.Shape;
import org.newdawn.slick.geom.Vector2f;

public class MainMenu extends BasicGame {

	ArrayList<Button> buttons;

	// void resize(500,500);
	boolean mainMenu = true;
	boolean options = false;

	public MainMenu(String gamename) {
		super(gamename);
	}

	@Override
	public void init(GameContainer gc) throws SlickException {

		buttons = new ArrayList<Button>();
		buttons.add(new Button(new Vector2f(150, 150), "teste", 35, 20,
				new CallBack() {
					public void run() {
						options = true;
						mainMenu = false;
						System.out.println("teste");
					}
				}));
	}

	@Override
	public void update(GameContainer gc, int i) throws SlickException {
		if (Mouse.isButtonDown(0)) {
			int x = Mouse.getX();
			int y = gc.getHeight() - Mouse.getY();

			for (Button button : buttons) {
				Shape spriteInSpace = new MorphShape(button.getSprite());
				spriteInSpace.setLocation(button.getPosition());

				if (spriteInSpace.contains(x, y)) {
					button.setClicked(true);
					button.getCallback().run();
				}
			}
		}
	}

	// System.out.println("Width: " + getWidth() / 2);
	// System.out.println("Height: " + getHeight() / 2);

	@Override
	public void render(GameContainer gc, org.newdawn.slick.Graphics g)
			throws SlickException {
		int height = gc.getHeight();
		int width = gc.getWidth();
		int divideBy = 2;
		int halfWidth = gc.getWidth() / divideBy;
		int halfHeight = gc.getHeight() / divideBy;

		 g.setBackground(Color.black);

		if (mainMenu) {
			g.setFont(new TrueTypeFont(new Font("TimesRoman", Font.PLAIN,
					height / 10), false));

			g.setColor(Color.yellow);
			g.drawString("Start", halfWidth - width / 10, halfHeight - height
					/ 5);

			g.setColor(Color.white);
			g.drawString("Options", halfWidth - width / 10, halfHeight);

			g.setColor(Color.red);
			g.drawString("Instructions", halfWidth - width / 10, halfHeight
					+ height / 5);
		}

		if (options) {
			g.setColor(Color.yellow);
			g.drawString("Sound", halfWidth - width / 10, halfHeight - height
					/ 5);

			g.setColor(Color.white);
			g.drawString("Controls", halfWidth - width / 10, halfHeight);

			g.setColor(Color.red);
			g.drawString("Back", halfWidth - width / 10, halfHeight + height
					/ 5);
		}

	}

	public static void main(String[] args) {
		try {
			AppGameContainer appgc;
			appgc = new AppGameContainer(new MainMenu("RunnerMan"));
			appgc.setDisplayMode(800, 500, false);
			// appgc.setShowFPS(false);

			appgc.start();
		} catch (SlickException ex) {
			Logger.getLogger(MainGame.class.getName()).log(Level.SEVERE, null,
					ex);
		}
	}

}