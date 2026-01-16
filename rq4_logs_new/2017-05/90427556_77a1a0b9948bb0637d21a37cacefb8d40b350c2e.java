import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Stroke;
import java.awt.geom.Ellipse2D;

public class Ring {
	
	private int x, y;
	private Ellipse2D.Double ellipse;
	private Color color;
	
	public Ring(int x, Color color) {
		this.x = x;
		this.y = 700;
		ellipse = new Ellipse2D.Double(x, y, 45, 30);
		this.color = color;
	}
	
	public Color getColor() { return color; }
	
	public void render(Graphics2D g){
		g.setColor(color);
		Stroke stroke = new BasicStroke(10, BasicStroke.CAP_BUTT, BasicStroke.CAP_ROUND, 0);
		g.setStroke(stroke);
		g.draw(ellipse);
	}
	
}