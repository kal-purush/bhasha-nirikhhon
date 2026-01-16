import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;

public class Apple extends Rectangle{

	private static final long serialVersionUID = 1L;

	public Apple(int x, int y){
		setBounds(x + 10,y + 8, 8, 8);
	}
	
	public void render(Graphics g){
		g.setColor(Color.green);
		g.fillRect(x, y, width, height);
	}
	
}