import java.awt.Canvas;
import java.awt.Color;
import java.awt.Graphics;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.WindowConstants;

public class B extends Canvas {
	
	private static final int SCALE = 10;

	public static void drawPicture(double[] values) {
		try {
			int dim = (int) Math.sqrt(values.length);
			double[][] newValues = new double[dim][dim];
			for (int i = 0; i < values.length; i++) {
				newValues[i % dim][i / dim] = values[i];
			}
			drawPicture(dim, newValues);	
		} catch (Exception e) {
			return;
		}
	}

	public static void drawPicture(int dim, double[][] newValues) {
		JFrame frame = new JFrame();
		frame.setSize(50 + Math.max(280, SCALE*dim), 50 + Math.max(280, SCALE*dim));
		frame.add(new B(newValues));
		frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		frame.setVisible(true);
	}
	
	private static final long serialVersionUID = 1480902466428347458L;
	private final int WIDTH;
	private final int HEIGHT;
	private double[][] values;

	public B(double[][] values) {
		super();
		this.WIDTH = values.length;
		this.HEIGHT = values[0].length;
		this.values = values;
	}

	@Override
	public void paint(Graphics g) {
		super.paint(g);
		for (int x = 0; x < WIDTH; x++) {
			for (int y = 0; y < HEIGHT; y++) {
				g.setColor(color(this.values[x][y]));
				for (int i = 0; i < SCALE; i++) {
					for (int j = 0; j < SCALE; j++) {
						g.drawLine(SCALE*x + i, SCALE*y + j, SCALE*x + i, SCALE*y + j);
					}	
				}
			}
		}
	}

	private float fromValue(double value) {
		return (float) (((300 * (1.0 - value) + 300) % 360) / 360.0);
	}

	private Color color(double value) {
		return Color.getHSBColor(fromValue(value), 1.0f, .5f);
	}
}