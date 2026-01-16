/*  +__^_________,_________,_____,________^-.-------------------,
 *  | |||||||||   `--------'     |          |                   O
 *  `+-------------USMC----------^----------|___________________|
 *    `\_,---------,---------,--------------'
 *      / X MK X /'|       /'
 *     / X MK X /  `\    /'
 *    / X MK X /`-------'
 *   / X MK X /
 *  / X MK X /
 * (________(                @author m.c.kunkel
 *  `------'
*/
package org.jlab.dc_calibration.domain;

import java.awt.Dimension;
import java.awt.Toolkit;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTabbedPane;

import org.jlab.groot.data.H1F;
import org.jlab.groot.graphics.EmbeddedCanvas;
import org.jlab.groot.math.F1D;
import org.jlab.groot.math.RandomFunc;

public class DCTabbed2DPane extends JFrame {
	private Dimension screensize;
	private JFrame frame;
	private JTabbedPane topTabbedPane;
	private JTabbedPane[] leftTabbedPane;
	private int rows;
	private int cols;

	private String frameName;

	public DCTabbed2DPane(String frameName, int rows, int cols) {
		this.frameName = frameName;
		this.rows = rows;
		this.cols = cols;
		init();
	}

	private void setScreenSize() {
		screensize = Toolkit.getDefaultToolkit().getScreenSize();
	}

	private void setJFrame() {
		frame = new JFrame(frameName);
		frame.setSize((int) (screensize.getHeight() * .75 * 1.618), (int) (screensize.getHeight() * .75));
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

	}

	private void setJTabbedPane() {
		topTabbedPane = new JTabbedPane(JTabbedPane.TOP);
		leftTabbedPane = new JTabbedPane[cols];

	}

	private void setTabs() {
		for (int i = 0; i < cols; i++) {
			leftTabbedPane[i] = new JTabbedPane(JTabbedPane.LEFT);
			for (int j = 0; j < rows; j++) {
				leftTabbedPane[i].addTab("Super Layer " + (j + 1), new JLabel("Sector " + (i + 1) + " - " + (j + 1)));
			}
			topTabbedPane.addTab("Sector " + (i + 1), leftTabbedPane[i]);
		}
		frame.add(topTabbedPane);
		topTabbedPane.setSelectedIndex(-1);
		System.out.println("here");

	}

	private void init() {
		setScreenSize();
		setJFrame();
		setJTabbedPane();
		setTabs();
	}

	public void addCanvasToPane(String name, EmbeddedCanvas can) {
		topTabbedPane.add(name, can);
	}

	public void addCanvasToPane(Integer row, EmbeddedCanvas can) {
		// frame.removeAll();
		leftTabbedPane[row].addTab("", can);
		frame.add(topTabbedPane);
		frame.revalidate();
		frame.repaint();
		// showFrame();
	}

	public void showFrame() {

		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
	}

	public static void main(String[] args) {

		F1D func = new F1D("func", "2.0+[a]*cos(x)+[b]*cos(2*x)", 0.0, 2.0 * 3.1415);
		func.setParameter(0, 0.5);
		func.setParameter(1, 1.0);
		RandomFunc randfunc = new RandomFunc(func);

		H1F h1 = new H1F("h1", "", 25, 0.0, 2.0 * 3.1415);

		for (int i = 0; i < 800; i++) {
			h1.fill(randfunc.random());
		}

		int rows = 6;
		int cols = 6;

		DCTabbed2DPane test = new DCTabbed2DPane("PooperDooper", rows, cols);
		// for (int i = 0; i < rows; i++) {
		for (int j = 0; j < cols; j++) {
			EmbeddedCanvas c0 = new EmbeddedCanvas();
			c0.clear();
			c0.cd(0);
			c0.draw(h1);
			test.addCanvasToPane(j, c0);
		}
		// }
		test.showFrame();

	}
}