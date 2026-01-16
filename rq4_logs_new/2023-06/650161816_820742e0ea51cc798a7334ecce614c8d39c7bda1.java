package tr.edu.medipol.yova;

public class PartiOylari {
	
	private static int partiAOy = 0;
	private static int partiBOy = 0;
	private static int partiCOy = 0;
	
	
	public static void partiOyArttir(String partiAdi) {
		if(partiAdi.equals("A")) {
			partiAOy++;
		}
		else if(partiAdi.equals("B")) {
			partiBOy++;
		}
		else if(partiAdi.equals("C")) {
			partiCOy++;
		}
		
	}
	
	public static void partiOyAzalt(String partiAdi) {
		if(partiAdi.equals("A")) {
			if(partiAOy!=0 || partiAOy>0) {
				partiAOy--;
			}
			else {
				
			}
		}
		else if(partiAdi.equals("B")) {
			if(partiBOy!=0 || partiBOy>0) {
				partiBOy--;
			}
			else {
				
			}
		}
		else if(partiAdi.equals("C")) {
			if(partiCOy!=0 || partiCOy>0) {
				partiCOy--;
			}
			else {
				
			}
		}
	
	}
	
	public static String partiOyListele() {
		return "Parti A: " + partiAOy + "\n" +
	           "Parti B: " + partiBOy + "\n" +
	           "Parti C: " + partiCOy + "\n";
	}
	
	public static int getPartiAOy() {
		return partiAOy;
	}
	
	public static int getPartiBOy() {
		return partiBOy;
	}
	
	public static int getPartiCOy() {
		return partiCOy;
	}



















}