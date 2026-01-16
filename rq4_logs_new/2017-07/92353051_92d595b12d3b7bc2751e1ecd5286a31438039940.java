package org.joltsphere.misc;

public class ObjectData {

	public String string;
	public int player = -1; // -1 by default for no players
	public int count;
	private boolean isDead = false;
	
	public ObjectData(String string) {
		this.string = string;
	}
	public ObjectData(String string, int player) {
		this(string);
		this.player = player;
	}
	public ObjectData(int count, String string) {
		this(string);
		this.count = count;
	}
	
	public void destroy() {
		isDead = true;
	}
	
	public boolean isDead() {
		return isDead; 
	}
	
	public boolean isPlayer() {
		if (player == -1) return false;
		else return true;
	}
	
}