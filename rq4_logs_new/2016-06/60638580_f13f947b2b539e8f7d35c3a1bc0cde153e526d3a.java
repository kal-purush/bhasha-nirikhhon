package com.frracm.asclepio.model.nif;

public class NIE implements SpanishNIF {
	private final char letraInicial;
	private final Integer numero;
	private final char letraFinal;
	private static char[] INITIAL_LETTER_NIE = { 'X', 'Y', 'Z' };

	public NIE(char letraInicial, Integer numero, char letraFinal) {
		this.letraInicial = letraInicial;
		this.numero = numero;
		this.letraFinal = letraFinal;
		if (!isValid()) {
			throw new IllegalArgumentException("NIE erróneo");
		}
	}

	public String getValue() {
		return (new StringBuilder()).append(letraInicial).append(numero).append(letraFinal).toString();
	}

	public boolean isValid() {
		String numberLength7 = String.format("%07d", numero);
		int initialLetterPosition = -1;
		for (int i = 0; i < INITIAL_LETTER_NIE.length; i++) {
			if (letraInicial == INITIAL_LETTER_NIE[i]) {
				initialLetterPosition = i;
			}
		}
		String completeNumberString = initialLetterPosition + numberLength7;
		Integer completeNumber = Integer.valueOf(completeNumberString);
		boolean response = true;
		int rest = completeNumber % 23;
		if (letraInicial == -1) {
			response = false;
		}
		if (letraFinal != LETTER_DNI[rest]) {
			response = false;
		}
		if (numero == null || numero > 9999999 || numero < 1) {
			response = false;
		}
		return response;
	}
}