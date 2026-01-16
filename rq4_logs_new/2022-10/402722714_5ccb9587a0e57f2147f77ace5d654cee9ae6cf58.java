package de.paulschnell.maushauen;

public class Maus {

	private int posX;
	private int posY;

	public Maus(int posX, int posY) {
		this.posX = posX;
		this.posY = posY;
	}

	public boolean IsGetroffen(int posX, int posY) {
		return this.posX == posX && this.posY == posY;
	}
}