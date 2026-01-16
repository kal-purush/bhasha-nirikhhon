package org.lang;

public class LanguageInfo {
	public void tamilLanguage() {
		System.out.println("Language Tamil");
	}
			public void englishlLanguage() {
			System.out.println("Language English");
		}
			public void hindilLanguage() {
				System.out.println("Language Hindi");
			}
			public static void main(String[] args) {
				LanguageInfo hh =new LanguageInfo();
				hh.tamilLanguage();
				hh.englishlLanguage();
				hh.hindilLanguage();
				StateInfo vv = new StateInfo();
				vv.northIndia();
				vv.southIndia();
				
			}

}