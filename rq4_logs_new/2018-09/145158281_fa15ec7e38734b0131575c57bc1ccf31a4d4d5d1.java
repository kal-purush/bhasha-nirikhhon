package shapes;

import abstractions.IShape;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;

public class OpenPolygon implements IShape{
	
	private Point firstPoint;
	private Point lastPoint;
	private Line lines;
	
	public OpenPolygon(){
		lines = new Line();
	}

	@Override
	public void setFirstPoint(Point p) {
		firstPoint = p;
		lines.setFirstPoint(firstPoint);
	}

	@Override
	public void setLastPoint(Point p) {
		lastPoint = p;
		lines.setLastPoint(lastPoint);
	}

	@Override
	public void draw(GraphicsContext gc, Color c, double diameter, double iterations) {
		lines.draw(gc,c,diameter,iterations);
		setFirstPoint(this.lastPoint);
	}
	
}