import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;

public class Guitar {

	private int x, y;
	private int width, height;
	private Rectangle guitar_board;
	private Color color;
	private GuitarString str1;
	private GuitarString str2;
	private GuitarString str3;
	private GuitarString str4;
	
	public Guitar() {
		this.width = 200;
		this.height = 700;
		this.x = GamePanel.getBoardWidth()/2 - (width/2);
		this.y = GamePanel.getBoardHeight()/2 - (height/2);
		this.color = new Color(255, 128, 0);
		guitar_board = new Rectangle(x, y, width, height);
		str1 = new GuitarString(x + 20, Color.BLUE);
		str2 = new GuitarString(x + 70, Color.BLUE);
		str3 = new GuitarString(x + 120, Color.BLUE);
		str4 = new GuitarString(x + 170, Color.BLUE);
	}
	
	public void render(Graphics2D g){
		g.setColor(color);
		g.fill(guitar_board);
		str1.render(g);
		str2.render(g);
		str3.render(g);
		str4.render(g);
	}
	
	
}