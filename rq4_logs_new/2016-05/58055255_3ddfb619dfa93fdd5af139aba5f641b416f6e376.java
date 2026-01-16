package fraktaler;

public class Penta extends Flake {

	@Override
	public void draw(Turtle turtle, int level, double size) {
		// prepare turtle for drawing 
		this.turtle = turtle;
		turtle.turnTo(180.0);
		// draw a pentagram
		for (int i = 0; i < 5; i++) {
			turtle.turn(72.0);
			drawSide(level, size);
		}	
	}
	
	private void drawSide(int level, double size) {

		// check if level > 0
		if (level > 0) {
			// draw figure calling drawSide recursively
			double s = size / 3.0;
			drawSide(level - 1, s);
			turtle.turn(108.0);
			drawSide(level - 1, s);
			turtle.turn(-72.0);
			drawSide(level - 1, s);
			turtle.turn(-72.0);
			drawSide(level - 1, s);
			turtle.turn(-72.0);
			drawSide(level - 1, s);
			turtle.turn(108.0);
			drawSide(level - 1, s);
		}
		// else, walk turtle
		else
			turtle.walk(size);
	}
}