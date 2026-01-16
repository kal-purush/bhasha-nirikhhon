package kafic;

import java.util.LinkedList;

import kafic.sistemskeoperacije.SODodajPice;

public class Kafic {

	private LinkedList<Pice> pice = new LinkedList<>();

	public void dodajPice(Pice naziv) {
		SODodajPice.izvrsi(pice, naziv);
	}
}