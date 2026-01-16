package desafio;

import java.util.Scanner;

public class anagrama {
	/* captura o valor das teclas */
	static Scanner entrada = new Scanner(System.in);

	public static void main(String[] args) {
		System.out.println("Digite a primeira palavra: ");
		String word1 = entrada.next();
		System.out.println("Digite a segunda palavra: ");
		String word2 = entrada.next();

		/*
		 * m�todo pra determinar se � um anagrama. se word1 � anagrama de word2
		 */
		if (isAnagrama(word1, word2))
			System.out.println("� anagrama");
		else
			System.out.println("N�o � anagrama");

		System.out.println("Opera��o finalizada");

	}

	/*
	 * Varrer todos os carecteres de word1 e word2. O charAt pega um caracter numa
	 * determinada posicao i e sua representacao � numerica. Essa representacao pode
	 * ser enxergada como um sequencia de bits. Com o operador '^' funciona da
	 * seguinte forma: dados dois bits de entrada.Se os dois bits forem iguais 1 e 1
	 * ou 0 e 0 o resultado � sempre zero(0).Caso os bits forem diferentes, o
	 * resultado � 1. Entao cada vez que aparecer uma letra da segunda palavra igual
	 * a primeira palavra. a varivavel 'r' ser� igual zero. Portanto, a variavel 'r'
	 * acumula zero. No final se o 'r' retornar zero � uma anagrama. Caso contrario,
	 * se d� igual a 1. N�o � anagrama
	 */
	private static boolean isAnagrama(String word1, String word2) {
		int r = 0;

		for (int i = 0; i < word1.length(); i++)
			r = r ^ word1.charAt(i);

		for (int i = 0; i < word2.length(); i++)
			r = r ^ word2.charAt(i);

		return (r == 0); // se retornar zero � verdadeiro, senao � falso
	}

}