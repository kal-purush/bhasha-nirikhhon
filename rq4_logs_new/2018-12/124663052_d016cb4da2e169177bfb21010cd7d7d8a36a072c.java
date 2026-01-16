package com.deff83.indexAPI;

public class AddOrder {
	private String isbid;
	private int zCoinadd,  notes_dialog;
	private String price_dialog;

	public AddOrder(String isbid, int zCoinadd, int notes_dialog, String price_dialog) {

		this.isbid = isbid;
		this.zCoinadd = zCoinadd;
		this.notes_dialog = notes_dialog;
		this.price_dialog = price_dialog;
	}

	public String getIsbid() {
		return isbid;
	}
	public void setIsbid(String isbid) {
		this.isbid = isbid;
	}
	public int getzCoinadd() {
		return zCoinadd;
	}
	public void setzCoinadd(int zCoinadd) {
		this.zCoinadd = zCoinadd;
	}
	public int getNotes_dialog() {
		return notes_dialog;
	}
	public void setNotes_dialog(int notes_dialog) {
		this.notes_dialog = notes_dialog;
	}
	public String getPrice_dialog() {
		return price_dialog;
	}
	public void setPrice_dialog(String price_dialog) {
		this.price_dialog = price_dialog;
	}
	
}