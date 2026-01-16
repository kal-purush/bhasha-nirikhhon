import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Stroke;
import java.awt.geom.Line2D;
import java.util.ArrayList;

public class Bars {

	private ArrayList<Line2D.Double> barLines;
	private Color color;
	private int width, height;
	
	public Bars(int width, int height) {
		barLines = new ArrayList<>();
		color = Color.BLACK;
		this.width = width;
		this.height = height;
	}
	
	public Color getColor() { return color; }
	
	public void render(Graphics2D g){
		for(int c = 50; c <= height; c += 100) {
			barLines.add(new Line2D.Double(GamePanel.getBoardWidth()/2 - (width/2), c, GamePanel.getBoardWidth()/2 - (width/2) + width, c));
		}
		g.setColor(color);
		Stroke stroke = new BasicStroke(10, BasicStroke.CAP_BUTT, BasicStroke.CAP_ROUND, 0);
		g.setStroke(stroke);
		for(int i = 0; i < barLines.size(); i++) {
			g.draw(barLines.get(i));
		}
	}
	
}