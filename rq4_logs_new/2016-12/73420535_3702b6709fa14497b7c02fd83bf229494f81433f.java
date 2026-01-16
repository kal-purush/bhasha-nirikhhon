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

import static org.jlab.dc_calibration.domain.Constants.nHists;
import static org.jlab.dc_calibration.domain.Constants.nLayer;
import static org.jlab.dc_calibration.domain.Constants.nSL;
import static org.jlab.dc_calibration.domain.Constants.nSectors;
import static org.jlab.dc_calibration.domain.Constants.nTh;
import static org.jlab.dc_calibration.domain.Constants.nThBinsVz;
import static org.jlab.dc_calibration.domain.Constants.thBins;
import static org.jlab.dc_calibration.domain.Constants.thEdgeVzH;
import static org.jlab.dc_calibration.domain.Constants.thEdgeVzL;
import static org.jlab.dc_calibration.domain.Histograms.h1ThSL;
import static org.jlab.dc_calibration.domain.Histograms.h1timeSlTh;
import static org.jlab.dc_calibration.domain.Histograms.h2timeVtrkDocaVZ;
//import static org.jlab.dc_calibration.domain.Histograms.*;
import static org.jlab.dc_calibration.domain.Histograms.hArrWire;
import static org.jlab.dc_calibration.domain.Histograms.htrkDoca;

import org.jlab.groot.base.TStyle;
import org.jlab.groot.data.H1F;
import org.jlab.groot.data.H2F;

public class InitializeHistograms {

	private String hTitle;
	private String hName;

	protected void initTBhists() {
		TStyle.createAttributes();
		for (int i = 0; i < nSL; i++) {
			for (int j = 0; j < nLayer; j++) {
				for (int k = 0; k < nHists; k++) {
					hName = "wireS" + (i + 1) + " L" + (j + 1) + " D" + k + "";
					hArrWire.put(new Coordinate(i, j, k), new H1F(hName, 120, -1.0, 119.0));
					hTitle = "wire (SL= " + (i + 1) + " Layer= " + (j + 1) + "DocaBin=" + k + ")";
					hArrWire.get(new Coordinate(i, j, k)).setTitleX(hTitle);
					hArrWire.get(new Coordinate(i, j, k)).setLineColor(i + 1);
				}
			}
		}
		for (int i = 0; i < nSectors; i++) {
			for (int j = 0; j < nSL; j++) {
				hName = "trkDocaSector" + (i + 1) + " superLayer" + (j + 1);
				htrkDoca.put(new Coordinate(i, j), new H1F(hName, 120, -3.0, 3.0));
				hTitle = "trkDoca (Sector= " + (i + 1) + " Superlayer= " + (j + 1) + ")";
				htrkDoca.get(new Coordinate(i, j)).setTitleX(hTitle);

			}
		}
	}

	protected void initTBSegments() {
		for (int i = 0; i < nSL; i++) {
			hName = "thetaSL " + (i + 1);
			hTitle = "#theta";
			h1ThSL.put(new Coordinate(i), new H1F(hName, 120, -60.0, 60.0));
			h1ThSL.get(new Coordinate(i)).setTitle(hTitle);
			h1ThSL.get(new Coordinate(0)).setLineColor(i + 1);
		}

		for (int i = 0; i < nSL; i++) {
			for (int k = 0; k < nTh; k++) {
				hName = "timeSL" + i + "ThBn" + k;
				h1timeSlTh.put(new Coordinate(i, k), new H1F(hName, 200, -10.0, 190.0));
				hTitle = String.format("time (SL=%d, th(%.1f,%.1f)", i + 1, thBins[k], thBins[k + 1]);
				hTitle = "time (SL=" + (i + 1) + " th(" + thBins[k] + "," + thBins[k + 1] + ")";
				h1timeSlTh.get(new Coordinate(i, k)).setTitleX(hTitle);
				h1timeSlTh.get(new Coordinate(i, k)).setLineColor(i + 1);
			}
		}

		for (int i = 0; i < nSectors; i++) {
			for (int j = 0; j < nSL; j++) {
				for (int k = 0; k < nThBinsVz; k++) { // nThBinsVz theta bins +/-2
					// deg around 0, 10, 20, 30,
					// 40, and 50 degs
					hName = "Sector" + (i + 1) + " timeVtrkDocaSL" + (j + 1) + "#theta Bin" + k;
					h2timeVtrkDocaVZ.put(new Coordinate(i, j, k), new H2F(hName, 200, 0.0, 1.0, 150, 0.0, 200.0));
					hTitle = "time vs. Doca (SL=" + (j + 1) + ", th(" + thEdgeVzL[k] + "," + thEdgeVzH[k] + "))"; // Worked
					h2timeVtrkDocaVZ.get(new Coordinate(i, j, k)).setTitle(hTitle);
					h2timeVtrkDocaVZ.get(new Coordinate(i, j, k)).setTitleX("Doca");
					h2timeVtrkDocaVZ.get(new Coordinate(i, j, k)).setTitleX("Time");

				}
			}
		}
	}
}