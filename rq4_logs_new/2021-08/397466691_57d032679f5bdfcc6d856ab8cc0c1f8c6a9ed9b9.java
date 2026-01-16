package com.yalco.singleton;

public class FirstPage {
	
	private Settings set = Settings.getSettings();
	
	public void setAndPrint() {
		set.setDarkMode(true);
		set.setFotSize(18);
		
		System.out.println(set.toString());
	}

}