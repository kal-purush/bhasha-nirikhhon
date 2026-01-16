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
package org.jlab.dc_calibration;

import javax.swing.ImageIcon;
import javax.swing.JLabel;

public class MyLabel {
	JLabel label;
	String path;
	String text;

	public MyLabel(String path, String text) {
		this.label = new JLabel();
		this.path = path;
		this.text = text;
	}

	public void setUpImage(String path) {
		this.label.setIcon(new ImageIcon(path));
	}

	public JLabel getLabel() {
		setUpImage(this.path);
		return this.label;
	}

}