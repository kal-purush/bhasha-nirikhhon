package gui;

import java.awt.Dimension;

import javax.swing.JFrame;

public class GUI {
	public static JFrame frame = new JFrame();

	public static Dimension screenSize;

	public GUI() {
		JFrame f = new JFrame("Swing Paint Demo");
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		f.add(new MyPanel());
		f.setSize(250, 250);
		f.setVisible(true);
	}
}