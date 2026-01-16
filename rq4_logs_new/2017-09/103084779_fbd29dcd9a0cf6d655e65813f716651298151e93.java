import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import javax.swing.*;

public class DrawRect extends JPanel {
	private static final int RECT_X = 20;
	private static final int RECT_Y = RECT_X;
	private static final int RECT_WIDTH = 100;
	private static final int RECT_HEIGHT = RECT_WIDTH;

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		// draw the rectangle here
		if (popupwindow.high) {
			g.setColor(Color.red);
			//popupwindow.high = false;
		}
		else if (popupwindow.medium) {
			g.setColor(Color.yellow);
			//popupwindow.medium = false;
		}
		else if (popupwindow.low) {
			g.setColor(Color.green);
			//popupwindow.low = false;
		}
		else {
			g.setColor(Color.white);
		}
		
		g.fillRect(RECT_X, RECT_Y, RECT_WIDTH, RECT_HEIGHT);
		g.setColor(Color.black);
		g.drawString(popupwindow.taskValue, RECT_X+40, RECT_Y+50);

	}

	@Override
	public Dimension getPreferredSize() {
		// so that our GUI is big enough
		return new Dimension(RECT_WIDTH + 2 * RECT_X, RECT_HEIGHT + 2 * RECT_Y);
	}

	// create the GUI explicitly on the Swing event thread
	public static void createAndShowGui() {

		DrawRect mainPanel = new DrawRect();
		MainClass.gui.getContentPane().add(mainPanel);
		MainClass.gui.pack();
		MainClass.gui.setLocationByPlatform(true);
		MainClass.gui.setVisible(true);
	}

}