package io.avalia.samba;

public class Piano implements IInstrument {

	public String play() {
		return "plink";
	}

	public int getSoundVolume() {
		return 26;
	}

	public String getColor() {
		return "brown";
	}

}