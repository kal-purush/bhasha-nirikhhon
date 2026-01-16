import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;

public class AIPaddle {
	
	int x;
	int y;
	int width = 15;
	int height = 40;
	int speed;
	boolean isTwoPlayer;
	boolean otherAI;
	Rectangle boundingBox;

	boolean goingUp = false;
	boolean goingDown = false;

	public AIPaddle(int x, int y) {
		this.x = x;
		this.y = y;
		
		boundingBox = new Rectangle(x, y, width, height);
		boundingBox.setBounds(x, y, width, height);
	}
	
	@SuppressWarnings("static-access")
	public void tick(Game game) {
		boundingBox.setBounds(x, y, width, height);
		
		if(!isTwoPlayer) {
			if(game.ball.y < y + height && y>= 0) {
				y -= speed;
			}
			if(game.ball.y > y && y + height <= game.getHeight()) {
				y += speed;
			}
		} else if(otherAI == true) {
			if(game.ball.y < y) {
				y -= speed;
			}
			if(game.ball.y > y) {
				y += speed;
			}
			
		} else {
			if(goingUp && y >= 0) {
				y -= speed;
			}else if(goingDown && y + height <= game.getHeight()) {
				y += speed;
			}
		}
	}
	public void render(Graphics g) {
		g.setColor(Color.WHITE);
		g.fillRect(x, y, width, height);
	}
}