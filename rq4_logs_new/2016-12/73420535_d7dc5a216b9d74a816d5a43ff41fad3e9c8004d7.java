package org.jlab.dc_monitoring;

import java.awt.Dimension;
import java.awt.Toolkit;

import javax.swing.JFrame;
import javax.swing.JTabbedPane;

import org.jlab.groot.data.H2F;
import org.jlab.groot.graphics.EmbeddedCanvas;

public class example {
	public static void main(String[] args) {
		Dimension screensize = Toolkit.getDefaultToolkit().getScreenSize();
		JFrame frame = new JFrame("DC MONITORING");
		frame.setSize((int) (screensize.getHeight() * .75 * 1.618), (int) (screensize.getHeight() * .75));
		JTabbedPane tabbedPane = new JTabbedPane();

		H2F[] cross_yvsx = new H2F[6];

		for (int i = 0; i < cross_yvsx.length; i++) {

			cross_yvsx[i] = new H2F("y vs x sector" + (i + 1), "", 100, -80, 80, 120, -100, 100);
			cross_yvsx[i].setTitleX("x");
			cross_yvsx[i].setTitleY("y");

		}

		/*
		 * EvioSource reader = new EvioSource(); reader.open(
		 * "/Users/omcortes/Documents/EVIOfiles/out_clasdispr.00.e11.000.emn0.75tmn.09.xs65.61nb.dis.10.evio"
		 * );
		 * 
		 * DC_Calib theDCcalib = new DC_Calib();
		 * 
		 * while (reader.hasEvent()) { DataEvent event = reader.getNextEvent();
		 * 
		 * if (event.hasBank("HitBasedTrkg::HBCrosses")) { DataBank
		 * hbcrossesBank = event.getBank("HitBasedTrkg::HBCrosses"); int
		 * nHBCrosses = hbcrossesBank.rows();
		 * 
		 * } }
		 */

		EmbeddedCanvas can4 = new EmbeddedCanvas();
		can4.divide(2, 3);
		for (int k = 0; k < cross_yvsx.length; k++) {
			can4.cd(k);
			can4.draw(cross_yvsx[k]);
		}

		tabbedPane.add("Crosses position", can4);
		frame.add(tabbedPane);
		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
		System.out.print("here");

	}

}