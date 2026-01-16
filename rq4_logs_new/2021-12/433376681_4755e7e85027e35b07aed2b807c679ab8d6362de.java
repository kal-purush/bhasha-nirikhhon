package demo;

import enumeration.Dimensione;
import enumeration.DirezioneV1;
import enumeration.DirezioneV2;
import enumeration.Livello;

public class DemoEnums {
	public static void main(String[] args) {
		
		// stampo/uso 1 valore
		System.out.println(DirezioneV1.EST);

		System.out.println("\n-------\n");
		
		// esempio 1 - enum classico
		for (DirezioneV1 valore : DirezioneV1.values()) {
			System.out.println(valore);
		}
		
		System.out.println("\n-------\n");
		
		// esempio 2 - enum con valori e codici
		for (DirezioneV2 valore : DirezioneV2.values()) {
			System.out.println(valore + ": " + valore.getCodice());
		}
		
		System.out.println("\n-------\n");
		
		// esempio 3 - enum valori con 2 parametri
		for (Livello valore : Livello.values()) {
			StringBuilder s = new StringBuilder();
			s.append(valore + ": ");
			s.append( "min " + valore.getMin());
			s.append( ", max " + valore.getMax());
			System.out.println(s.toString());
		}
		
		System.out.println("\n-------\n");

		// esempio 4 - override dei toString dei valori enum
		
		System.out.println(Dimensione.PICCOLA);
		System.out.println(Dimensione.GRANDE);
		
	}
}