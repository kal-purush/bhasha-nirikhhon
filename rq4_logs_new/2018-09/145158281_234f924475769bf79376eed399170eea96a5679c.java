package app;

import abstractions.IShape;
import gui.CanvasGUI;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import shapes.Point;

public class Drawer {
	
	private int openPoints;
	private IShape shape;
	private CanvasGUI canvas;
	
	public Drawer(CanvasGUI canvas) {
		this.canvas = canvas;
	}
	
	public void drawShape(MouseEvent e, Color drawColor, GraphicsContext gc, double thickness, double iterations){

		Point p = canvas.getPoint(e);

		p.setColor(drawColor);
		p.drawPoint(gc);
		setupPoint(p);
		if(openPoints > 1){
			shape.draw(gc, drawColor, thickness, iterations);
		}
		refreshPoint();
	}
	
	public void drawPoint(MouseEvent e, Color drawColor, GraphicsContext gc, double thickness){
		int x, y;
		
		x = (int)e.getX();
		y = (int)e.getY();
		Point p = new Point(x, y, thickness);
		p.setColor(drawColor);
		p.drawPoint(gc);
	}
	
	public void drawMoreThanTwoPoints(MouseEvent e, Color drawColor, GraphicsContext gc, double thickness, double iterations){
		
		Point p;
		if(openPoints == 0){
			p = canvas.getPoint(e);
			p.setColor(drawColor);
			p.drawPoint(gc);
			shape.setFirstPoint(p);
			openPoints++;
		}
		else{
			p = canvas.getPoint(e);
			p.setColor(drawColor);
			p.drawPoint(gc);
			shape.setLastPoint(p);
			shape.draw(gc, drawColor, thickness, iterations);
			canvas.disableMenus(false);
		}
	}
	
	private void setupPoint(Point p){
		if(openPoints == 0) {
			shape.setFirstPoint(p);
			canvas.disableMenus(true);
		}
		else if(openPoints == 1) {
			shape.setLastPoint(p);
		}
		openPoints++;
	}
	
	private void refreshPoint(){
		if(openPoints == 2) {
			openPoints = 0;
			canvas.disableMenus(false);
		}
	}
	
	public IShape getShape() {
		return shape;
	}
	
	public void setShape(IShape shape) {
		this.shape = shape;
	}

	public int getOpenPoints() {
		return openPoints;
	}

	public void setOpenPoints(int openPoints) {
		this.openPoints = openPoints;
	}
	
	

}