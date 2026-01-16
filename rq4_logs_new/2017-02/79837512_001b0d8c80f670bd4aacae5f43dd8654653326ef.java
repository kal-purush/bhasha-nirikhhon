package com.sj1.pr3.passgen;

import java.util.Random;

public class PassGen {

	/** Variabler för Gemener, Versaler, Siffror och Symboler. */
	private static final String UPR = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final String LWR = "abcdefghijklmnopqrstuvwxyz";
	private static final String NBR = "0123456789";
	private static final String SYM = "!@#$%^&*_=+-/";

	/**
	 * Denna konstruktor tar 6 inparametrar för längd min samt max, antal
	 * siffror, symboler, versaler samt gemener. Om min längd är större än max
	 * längd, kastar den en Exception.
	 */
	public static char[] genPwd(int minLength, int maxLength, int nbrOfUPR, int nbrOfLWR, int nbrOfNBR, int nbrOfSYM) {
		if (minLength > maxLength) {
			throw new IllegalArgumentException("Min length cannot be larger than max.");
		}

		Random rnd = new Random();
		int length = rnd.nextInt(maxLength - minLength + 1) + minLength;
		char[] passw = new char[length];

		/** Funktion som lägger till en slumpmässig versal i lösenordet. */
		int index = 0;
		for (int i = 0; i < nbrOfUPR; i++) {
			index = getNextIndex(rnd, length, passw);
			passw[index] = UPR.charAt(rnd.nextInt(UPR.length()));
		}
		/** Funktion som lägger till en slumpmässigt nummer i lösenordet. */
		for (int i = 0; i < nbrOfNBR; i++) {
			index = getNextIndex(rnd, length, passw);
			passw[index] = NBR.charAt(rnd.nextInt(NBR.length()));
		}
		/** Funktion som lägger till en slumpmässig symbol i lösenordet */
		for (int i = 0; i < nbrOfSYM; i++) {
			index = getNextIndex(rnd, length, passw);
			passw[index] = SYM.charAt(rnd.nextInt(SYM.length()));
		}
		for (int i = 0; i < length; i++) {
			if (passw[i] == 0) {
				passw[i] = LWR.charAt(rnd.nextInt(LWR.length()));
			}
		}

		return passw;
	}

	/** En statisk metod med tre inparametrar för att returnera nästa i passw */
	private static int getNextIndex(Random rnd, int length, char[] passw) {
		int index = rnd.nextInt(length);
		while (passw[index = rnd.nextInt(length)] != 0)
			;
		return index;

	}
}