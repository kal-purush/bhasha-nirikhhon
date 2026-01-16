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
import javax.swing.JTabbedPane;

import org.jlab.groot.graphics.EmbeddedCanvas;

public class DCTabbedPane {

	private Dimension screensize = null;
	private JFrame frame = null;
	private JTabbedPane tabbedPane = null;
	private String frameName;

	public DCTabbedPane(String frameName) {
		init();
		this.frameName = frameName;
	}

	private void setScreenSize() {
		screensize = Toolkit.getDefaultToolkit().getScreenSize();
	}

	private void setJFrame() {
		frame = new JFrame(frameName);
		frame.setSize((int) (screensize.getHeight() * .75 * 1.618), (int) (screensize.getHeight() * .75));
	}

	private void setJTabbedPane() {
		tabbedPane = new JTabbedPane();
	}

	private void init() {
		setScreenSize();
		setJFrame();
		setJTabbedPane();
	}

	public void addCanvasToPane(String name, EmbeddedCanvas can) {
		tabbedPane.add(name, can);
	}

	public void showFrame() {
		frame.add(tabbedPane);
		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
	}

	public static void main(String[] args) {

		DCTabbedPane test = new DCTabbedPane("PooperDooper");
		for (int i = 0; i < 10; i++) {
			test.addCanvasToPane("sector " + (i + 1), new EmbeddedCanvas());
		}
		test.showFrame();

	}
}