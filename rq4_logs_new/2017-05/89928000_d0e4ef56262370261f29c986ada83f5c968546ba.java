package mower.model;

import static java.lang.String.format;
import static mower.model.Orientation.left;
import static mower.model.Orientation.right;

public class Mower {
	private Orientation orientation;
	private int x;
	private int y;
	
	public Mower(String rawLine) {
		String[] arr = rawLine.split(" ");
		this.x = Integer.valueOf(arr[0]);
		this.y = Integer.valueOf(arr[1]);
		this.orientation = Orientation.valueOf(arr[2]);
	}
	
	public void moveY(int i) {
		y += i;
	}
	
	public void moveX(int i) {
		x += i;
	}
	
	public String getPosition() {
		return format("%s %s %s", x, y, orientation.name());
	}

	public Orientation getOrientation() {
		return orientation;
	}

	public void setOrientation(String command) {
		switch (command) {
			case "L":
				orientation = left(orientation);
				break;
			case "R":
				orientation = right(orientation);
				break;
			default:
				throw new IllegalStateException();
		}
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}
}