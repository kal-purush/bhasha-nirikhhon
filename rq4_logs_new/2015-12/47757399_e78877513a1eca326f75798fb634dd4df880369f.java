package ui;

import java.awt.Color;

public class Fader implements Runnable {
	javax.swing.JLabel label;
	Color c;

	Fader(javax.swing.JLabel label) {
		this.label = label;
		c = label.getForeground();
	}

	public void run() {
		int alpha = label.getForeground().getAlpha();
		while (true) {
			// System.out.println(alpha);
			while (alpha > 0) {
				alpha -= 25;
				if(alpha < 0) alpha =0;
				label.setForeground(new Color(c.getRed(), c.getGreen(), c.getBlue(), alpha));
				label.repaint();
				try {
					Thread.sleep(50);
				} catch (InterruptedException ex) {
					// Logger.getLogger(Fader.class.getName()).log(Level.SEVERE,
					// null, ex);
				}
			}
			while (alpha < 255) {
				alpha += 25;
				if(alpha > 255) alpha =255;
				label.setForeground(new Color(c.getRed(), c.getGreen(), c.getBlue(), alpha));
				label.repaint();
				try {
					Thread.sleep(50);
				} catch (InterruptedException ex) {
					// Logger.getLogger(Fader.class.getName()).log(Level.SEVERE,
					// null, ex);
				}
			}

		}
	}
}