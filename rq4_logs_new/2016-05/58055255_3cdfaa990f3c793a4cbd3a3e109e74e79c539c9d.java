package fraktaler;

public class Sierpinski extends Flake {

	@Override
	public void draw(Turtle turtle, int level, double size) {
		// prepare turtle for drawing
		this.turtle = turtle;
		turtle.turnTo(0.0);
		turtle.turn(-60.0);
		// draw Sierpinski triangle
		drawSide(level, size);
	}
	
	private void drawSide(int level, double size) {
		// check if level > 0
		if (level > 0) {
			// draw figure calling drawSide recursively
			double s = size / 2.0;
			drawSide(level - 1, s);
			turtle.jump(s);
			drawSide(level - 1, s);
			turtle.turn(120.0);
			turtle.jump(s);
			turtle.turn(-120.0);
			drawSide(level - 1, s);
			turtle.turn(-120.0);
			turtle.jump(s);
			turtle.turn(120.0);
		}
		// else, draw simple triangle
		else {
			for (int i = 0; i < 3; i++) {
				turtle.walk(size);
				turtle.turn(120);
			}
		}
	}
}