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

import java.util.ArrayList;

import org.jlab.groot.data.H1F;
import org.jlab.groot.math.F1D;
import org.jlab.groot.math.RandomFunc;
import org.jlab.groot.ui.TCanvas;

public class ArrayOf1DHistogram {

	private H1F histogram = new H1F("histogram", "", 10, -1000, 1000);
	private int x, y, z;

	public ArrayOf1DHistogram(int x, H1F histogram) {
		this.x = x;
		this.histogram = histogram;
	}

	public ArrayOf1DHistogram(int x, int y, H1F histogram) {
		this(x, histogram);
		this.y = y;
	}

	public ArrayOf1DHistogram(int x, int y, int z, H1F histogram) {
		this(x, y, histogram);
		this.z = z;
	}

	public static ArrayOf1DHistogram create(int x, int y, int z, H1F histogram) {
		return new ArrayOf1DHistogram(x, y, z, histogram);
	}

	public static ArrayOf1DHistogram create(int x, int y, H1F histogram) {
		return new ArrayOf1DHistogram(x, y, histogram);
	}

	public static ArrayOf1DHistogram create(int x, H1F histogram) {
		return new ArrayOf1DHistogram(x, histogram);
	}

	public H1F getHistogram() {
		return histogram;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + x;
		result = prime * result + y;
		result = prime * result + z;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArrayOf1DHistogram other = (ArrayOf1DHistogram) obj;
		if (x != other.x)
			return false;
		if (y != other.y)
			return false;
		if (z != other.z)
			return false;
		return true;
	}

	public static void main(String[] args) {

		F1D func = new F1D("func", "2.0+[a]*cos(x)+[b]*cos(2*x)", 0.0, 2.0 * 3.1415);
		func.setParameter(0, 0.5);
		func.setParameter(1, 1.0);
		RandomFunc randfunc = new RandomFunc(func);
		ArrayList<ArrayOf1DHistogram> ArH1 = new ArrayList<ArrayOf1DHistogram>();

		for (int i = 0; i < 3; i++) {
			ArrayOf1DHistogram temp = new ArrayOf1DHistogram(i, new H1F("h" + 1, "", 50, 0.0, 2.0 * Math.PI));
			ArH1.add(temp);
		}
		ArH1.get(1).getHistogram().setTitle("This title");
		ArH1.get(1).getHistogram().setTitleX("This title");
		ArH1.get(1).getHistogram().setTitleY("This title");

		for (int i = 0; i < 1800; i++) {
			ArH1.get(1).getHistogram().fill(randfunc.random());
		}
		TCanvas c1 = new TCanvas("c1", 800, 800);
		c1.draw(ArH1.get(1).getHistogram());
		//
		// H1F h1 = new H1F("h1", "", 50, 0.0, 2.0 * 3.1415);
		//
		// for (int i = 0; i < 1800; i++) {
		// h1.fill(randfunc.random());
		// }
		// c1.divide(1, 2);
		//
		// c1.cd(0);
		// c1.draw(ArH1.get(1).getHistogram());
		//
		// c1.cd(1);
		// c1.draw(h1);

		// ArrayOf1DHistogram temp = new ArrayOf1DHistogram(1, 2, new H1F("h1", "", 50, 0.0, 2.0 * 3.1415));
		// H1F trial = temp.getHistogram();
		// for (int i = 0; i < 1800; i++) {
		// trial.fill(randfunc.random());
		// }
		// TCanvas c1 = new TCanvas("c1", 800, 800);
		// c1.draw(trial);
	}

}