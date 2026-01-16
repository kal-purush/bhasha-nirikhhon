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
 *  `------'				 @author Olga Cortes
*/
package org.jlab.dc_monitoring;

import java.awt.Dimension;
import java.awt.Toolkit;

import javax.swing.JFrame;
import javax.swing.JTabbedPane;

import org.jlab.groot.data.H2F;
import org.jlab.io.base.DataBank;
import org.jlab.io.base.DataEvent;
import org.jlab.io.evio.EvioSource;

public class DCMonitoring {

	private String fileName;

	private Dimension screensize = null;
	private JFrame frame = null;
	private JTabbedPane tabbedPane = null;

	private H2F[] occupancies = null;
	private H2F[] trkdocasvstime = null;
	private H2F[] cross_yvsx = null;
	private H2F[] cross_uyvsux = null;
	private H2F thetaVSmomenutm = null;
	private H2F phiVSmomenutm = null;
	private H2F thetaVSphi = null;

	private EvioSource reader = null;

	public DCMonitoring() {
		init();
		reader.open("/Users/omcortes/Documents/EVIOfiles/out_clasdispr.00.e11.000.emn0.75tmn.09.xs65.61nb.dis.10.evio");
	}

	public DCMonitoring(String fileName) {
		this.fileName = fileName;
		init();
		reader.open(fileName);
	}

	private void setScreenSize() {
		screensize = Toolkit.getDefaultToolkit().getScreenSize();
	}

	private void setJFrame() {
		frame = new JFrame("DC MONITORING");
		frame.setSize((int) (screensize.getHeight() * .75 * 1.618), (int) (screensize.getHeight() * .75));
	}

	private void setJTabbedPane() {
		tabbedPane = new JTabbedPane();
	}

	private void createHistograms() {
		occupancies = new H2F[36];
		trkdocasvstime = new H2F[36];
		cross_yvsx = new H2F[6];
		cross_uyvsux = new H2F[6];
		thetaVSmomenutm = new H2F("theta vs momentum", "", 100, 0.0, 10.0, 150, 0, 2);
		phiVSmomenutm = new H2F("phi vs momentum", "", 100, 0.0, 10.0, 150, -Math.PI, Math.PI);
		thetaVSphi = new H2F("theta vs phi", "", 150, -Math.PI, Math.PI, 150, 0, 2);

		thetaVSmomenutm.setTitleX("p (GeV/c)");
		thetaVSmomenutm.setTitleY("#theta(rad)");
		thetaVSphi.setTitleX("#phi (rad)");
		thetaVSphi.setTitleY("#theta(rad)");
		phiVSmomenutm.setTitleX("p (GeV/c)");
		phiVSmomenutm.setTitleY("#phi(rad)");

		for (int j = 0; j < occupancies.length; j++) {
			occupancies[j] = new H2F("Occupancy" + j, "", 112, 1, 113, 6, 1, 7);
			occupancies[j].setTitleX("Wire");
			occupancies[j].setTitleY("Layer");
			if ((j % 6 == 0) || (j % 6 == 1)) {
				trkdocasvstime[j] = new H2F("TrackDoca vs time" + j, "", 100, 0, 200, 100, 0, 1);
			} else
				trkdocasvstime[j] = new H2F("TrackDoca vs time" + j, "", 100, 0, 600, 100, 0, 2);

		}

		for (int i = 0; i < cross_yvsx.length; i++) {
			cross_yvsx[i] = new H2F("y vs x sector" + (i + 1), "", 180, -180, 180, 120, -140, 140);
			cross_yvsx[i].setTitleX("x");
			cross_yvsx[i].setTitleY("y");
			cross_uyvsux[i] = new H2F("Uy vs Ux sector" + (i + 1), "", 100, -1, 1, 100, -1, 1);
			cross_uyvsux[i].setTitleX("Ux");
			cross_uyvsux[i].setTitleY("Uy");

		}

	}

	private void init() {
		setScreenSize();
		setJFrame();
		setJTabbedPane();
		createHistograms();
		reader = new EvioSource();
	}

	private void processEvent() {
		while (reader.hasEvent()) {
			DataEvent event = reader.getNextEvent();
			if (event.hasBank("TimeBasedTrkg::TBHits")) {
				processTBHits(event);
			}
		}

	}

	private void processTBHits(DataEvent event) {
		DataBank tbhitsBank = event.getBank("TimeBasedTrkg::TBHits");
		int nTBHits = tbhitsBank.rows();
		for (int i = 0; i < nTBHits; i++) {
			if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[0].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[0].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[1].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[1].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[2].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[2].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[3].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[3].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[4].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[4].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 1 && tbhitsBank.getInt("superlayer", i) == 6) {
				occupancies[5].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[5].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[6].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[6].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[7].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[7].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[8].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[8].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[9].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[9].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[10].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[10].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 2 && tbhitsBank.getInt("superlayer", i) == 6) {
				occupancies[11].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[11].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[12].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[12].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[13].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[13].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[14].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[14].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[15].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[15].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[16].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[16].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 3 && tbhitsBank.getInt("superlayer", i) == 6) {
				occupancies[17].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[17].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[18].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[18].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[19].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[19].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[20].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[20].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[21].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[21].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[22].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[22].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 4 && tbhitsBank.getInt("superlayer", i) == 6) {
				occupancies[23].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[23].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[24].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[24].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[25].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[25].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[26].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[26].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[27].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[27].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[28].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[28].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 5 && tbhitsBank.getInt("superlayer", i) == 6) {
				occupancies[29].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[29].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 6 && tbhitsBank.getInt("superlayer", i) == 1) {
				occupancies[30].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[30].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 6 && tbhitsBank.getInt("superlayer", i) == 2) {
				occupancies[31].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[31].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 6 && tbhitsBank.getInt("superlayer", i) == 3) {
				occupancies[32].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[32].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 6 && tbhitsBank.getInt("superlayer", i) == 4) {
				occupancies[33].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[33].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else if (tbhitsBank.getInt("sector", i) == 6 && tbhitsBank.getInt("superlayer", i) == 5) {
				occupancies[34].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[34].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			} else {
				occupancies[35].fill(tbhitsBank.getInt("wire", i), tbhitsBank.getInt("layer", i));
				trkdocasvstime[35].fill(tbhitsBank.getDouble("time", i), tbhitsBank.getDouble("trkDoca", i));
			}

		}

	}

}