/**
 * Jogo da Velha (Exercicio 6 | Curso Java básico - Loiane Groner)
 * autor: Vinicius Barbosa
 * versao: 07/04/2018
 * Jogo Completo - Porém ainda falta melhorar muitas coisas(organização e redução de código)
 */
import java.util.Scanner;

public class JogoDaVelha {

	public static void main(String[] args) {

		Scanner scan = new Scanner(System.in);
		
		String[][] tabuleiro = new String[3][3];
		
		for (int i = 0; i < tabuleiro.length; i++) {
			for (int j = 0; j < tabuleiro[i].length; j++) {
				tabuleiro[i][j] = " ";
			}
		}

		System.out.println();

		boolean tabuleiroCheio = false, jogador1Ganhou = false, jogador2Ganhou = false, posValida, deuVelha;
		int posicao = 0;
		while (true) {

			// IMPRIMINDO O  TABULEIRO
			for (int i = 0; i < tabuleiro.length; i++) {
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (j < tabuleiro[i].length - 1) {
						System.out.print(tabuleiro[i][j] + " |");
					} else {
						System.out.print(tabuleiro[i][j]);
					}
					
				}
				if (i < tabuleiro.length - 1) {
					System.out.print("\n--------\n");
				}
				
			}
			System.out.println();
			System.out.println();


			System.out.println("Jogador 1 - 'O' ");
			
			posValida = false;
			while (!posValida) {
				System.out.print("Posição da peça: ");
				posicao = scan.nextInt();
				if (posicao > 0 && posicao <= 9) {
					
					if (posicao == 1) {
						if (tabuleiro[0][0] == " ") {
							tabuleiro[0][0] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 2) {
						if (tabuleiro[0][1] == " ") {
							tabuleiro[0][1] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 3) {
						if (tabuleiro[0][2] == " ") {
							tabuleiro[0][2] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 4) {
						if (tabuleiro[1][0] == " ") {
							tabuleiro[1][0] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 5) {
						if (tabuleiro[1][1] == " ") {
							tabuleiro[1][1] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 6) {
						if (tabuleiro[1][2] == " ") {
							tabuleiro[1][2] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 7) {
						if (tabuleiro[2][0] == " ") {
							tabuleiro[2][0] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 8) {
						if (tabuleiro[2][1] == " ") {
							tabuleiro[2][1] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 9) {
						if (tabuleiro[2][2] == " ") {
							tabuleiro[2][2] = "O";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					}
				} else{
					System.out.println("Posição inválida!! Tente novamente.");
				}
				
			}

			// verificando se há campeao nas linhas horizontais
			int cont;
			jogador1Ganhou = false;
			for (int i = 0; i < tabuleiro.length ; i++) {
				cont = 0;
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[i][j].equals("O")) {
						cont++;
					}
				}
				if (cont == 3) {
					jogador1Ganhou = true;
				}
			}

			// verificando se há campeao nas linhas verticais
			for (int i = 0; i < tabuleiro.length ; i++) {
				cont = 0;
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[j][i].equals("O")) {
						cont++;
					}
				}
				if (cont == 3) {
					jogador1Ganhou = true;
				}
			}

			// verificando se há campeao nas diagonais
			cont = 0;
			for (int i = 0; i < tabuleiro.length ; i++) {
				if (tabuleiro[i][i].equals("O")) {
					cont++;
				}			
				if (cont == 3) {
					jogador1Ganhou = true;
				}
			}
			if (tabuleiro[2][0] == tabuleiro[1][1] && tabuleiro[1][1] == tabuleiro[0][2] && tabuleiro[2][0].equals("O")) {
				jogador1Ganhou = true;
			}



			// IMPRIMINDO O  TABULEIRO
			for (int i = 0; i < tabuleiro.length; i++) {
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (j < tabuleiro[i].length - 1) {
						System.out.print(tabuleiro[i][j] + " |");
					} else {
						System.out.print(tabuleiro[i][j]);
					}
					
				}
				if (i < tabuleiro.length - 1) {
					System.out.print("\n--------\n");
				}
				
			}
			System.out.println();
			System.out.println();

			if (jogador1Ganhou) {
				System.out.println("Jogador 1 (O) venceu!");
				break;
			}

			// VERIFICANDO SE HÁ ESPAÇO NO TABULEIRO
			deuVelha = true;
			for (int i = 0; i < tabuleiro.length; i++) {
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[i][j].equals(" ")) {
						deuVelha = false;
					}
				}
			}
			if (deuVelha) {
				System.out.println("Deu velha.");
				break;
			}


			System.out.println("Jogador 2 - 'X' ");
	
			posValida = false;
			while (!posValida) {
				System.out.print("Posição da peça: ");
				posicao = scan.nextInt();
				if (posicao > 0 && posicao <= 9) {
					
					if (posicao == 1) {
						if (tabuleiro[0][0] == " ") {
							tabuleiro[0][0] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 2) {
						if (tabuleiro[0][1] == " ") {
							tabuleiro[0][1] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 3) {
						if (tabuleiro[0][2] == " ") {
							tabuleiro[0][2] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 4) {
						if (tabuleiro[1][0] == " ") {
							tabuleiro[1][0] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 5) {
						if (tabuleiro[1][1] == " ") {
							tabuleiro[1][1] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 6) {
						if (tabuleiro[1][2] == " ") {
							tabuleiro[1][2] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 7) {
						if (tabuleiro[2][0] == " ") {
							tabuleiro[2][0] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 8) {
						if (tabuleiro[2][1] == " ") {
							tabuleiro[2][1] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					} else if (posicao == 9) {
						if (tabuleiro[2][2] == " ") {
							tabuleiro[2][2] = "X";
							posValida = true;
						} else {
							System.out.println("\nPosição inválida!! Tente novamente.\n");
						}
					}
				} else{
					System.out.println("\nPosição inválida!! Tente novamente.\n");
				}
				
			}


			// verificando se há campeao nas linhas horizontais
			jogador2Ganhou = false;
			for (int i = 0; i < tabuleiro.length ; i++) {
				cont = 0;
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[i][j].equals("X")) {
						cont++;
					}
				}
				if (cont == 3) {
					jogador2Ganhou = true;
				}
			}

			// verificando se há campeao nas linhas verticais
			for (int i = 0; i < tabuleiro.length ; i++) {
				cont = 0;
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[j][i].equals("X")) {
						cont++;
					}
				}
				if (cont == 3) {
					jogador2Ganhou = true;
				}
			}

			// verificando se há campeao nas diagonais
			for (int i = 0; i < tabuleiro.length ; i++) {
				cont = 0;
				if (tabuleiro[i][i].equals("X")) {
					cont++;
				}			
				if (cont == 3) {
					jogador2Ganhou = true;
				}
			}
			if (tabuleiro[2][0] == tabuleiro[1][1] && tabuleiro[1][1] == tabuleiro[0][2] && tabuleiro[2][0].equals("X")) {
				jogador2Ganhou = true;
			}

			// IMPRIMINDO O  TABULEIRO
			for (int i = 0; i < tabuleiro.length; i++) {
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (j < tabuleiro[i].length - 1) {
						System.out.print(tabuleiro[i][j] + " |");
					} else {
						System.out.print(tabuleiro[i][j]);
					}
					
				}
				if (i < tabuleiro.length - 1) {
					System.out.print("\n--------\n");
				}
				
			}
			System.out.println();
			System.out.println();

			if (jogador2Ganhou) {
				System.out.println("Jogador 2 (X) venceu!");
				break;
			}


			// VERIFICANDO SE HÁ ESPAÇO NO TABULEIRO
			deuVelha = true;
			for (int i = 0; i < tabuleiro.length; i++) {
				for (int j = 0; j < tabuleiro[i].length; j++) {
					if (tabuleiro[i][j].equals(" ")) {
						deuVelha = false;
					}
				}
			}
			if (deuVelha) {
				System.out.println("Deu velha.");
				break;
			}

		}
		
	}

}
