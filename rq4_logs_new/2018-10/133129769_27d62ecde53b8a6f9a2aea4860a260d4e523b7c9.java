package com.configreader.configreader.utils;

public class OSCheck {

	private static final String DEF_FOLDER = "drivers/";
	private static final String OS = System.getProperty("os.name").toLowerCase();

	public void setup() {

		StringBuilder sb = new StringBuilder(DEF_FOLDER);
		sb.append("chromedriver");
		if (OS.contains("windows")) {
			sb.append(".exe");

		}
		final String path = getClass().getClassLoader().getResource(sb.toString()).getPath();
		System.setProperty("webdriver.chrome.driver", path);
	}

}