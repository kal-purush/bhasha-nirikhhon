package com.frracm.asclepio.model.nif;

public class DNI {
	private static char[] LETTER_DNI = { 'T', 'R', 'W', 'A', 'G', 'M', 'Y', 'F', 'P', 'D', 'X', 'B', 'N', 'J', 'Z', 'S',
			'Q', 'V', 'H', 'L', 'C', 'K', 'E' };
	private final Integer numero;
	private final char letra;

	public DNI(Integer numero, char letra) {
		this.numero = numero;
		this.letra = letra;
		if (!isValid()) {
			throw new IllegalArgumentException("DNI erróneo");
		}
	};

	public String getValue() {
		return numero.toString() + letra;
	}

	private boolean isValid() {

		int rest = numero % 23;
		if (letra == LETTER_DNI[rest]) {
			return true;
		} else {
			return false;
		}
	}
}