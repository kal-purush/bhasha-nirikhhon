package br.ufsc.ine5605.grupo3.apresentacao;

import java.io.IOException;
import java.util.Calendar;
import java.util.Scanner;

import br.ufsc.ine5605.grupo3.controladores.ControladorChave;
import br.ufsc.ine5605.grupo3.entidades.Chave;
import br.ufsc.ine5605.grupo3.entidades.Funcionario;

public class TelaChave implements Tela {
	private int opcao;
	private Scanner sc;
	private ControladorChave owner;

	public TelaChave(ControladorChave owner) {
		this.owner = owner;
		this.sc = new Scanner(System.in);
	}

	@Override
	public void exibirTela() {
		do {
			cls();
			System.out.println("\n Bem Vindo ao Claviculário");
			System.out.println("Digite 1 para Exibir Chaves");
			System.out.println("Digite 2 para Pegar Chave");
			System.out.println("Digite 3 para Devolver Chave");
			System.out.println("Digite 0 Para retornar para o Menu Principal");
			this.opcao = this.sc.nextInt();
			this.tratadorOpcao();
		} while (this.opcao != 0);
	}

	public void tratadorOpcao() {
		cls();
		switch (this.opcao) {
		case 1:
			System.out.println("Lista de Chaves");
			System.out.println("-----------------");
			this.owner.exibirChaves();
			break;
		case 2:
			this.exibeTelaPegar();
			break;
		case 3:
			this.exibeTelaDevolver();
			break;
		default:
			break;
		}
	}

	public void exibeTelaExibicao(Chave Chave, int index) {
		System.out.println("Chave nº " + index + "\nPlaca: " + Chave.getPlaca() + "\nSituação: " + (Chave.isAlugada() ? "Alugada" : "Disponível"));
		System.out.println("-----------------");
	}

	private void exibeTelaPegar() {
		try {
			int tentativas = 0;
			Funcionario f = null;
			cls();

			while (tentativas < 3) {
				System.out.println("Digite a sua matrícula");
				int matricula = this.sc.nextInt();
				f = this.owner.pegaFuncionario(matricula);

				if (f == null) {
					System.out.println("Matricula não existe");
					this.owner.adicionarRegistro(Calendar.getInstance().getTime().getDate(), Calendar.getInstance().getTime().getMonth(),
							Calendar.getInstance().getTime().getHours(), f, null, false, "Matricula não existente");
				} else {
					break;
				}
				tentativas++;
			}

			if (tentativas >= 3) {
				System.out.println("Tentativas demais com matricula errada");
				return;
			}

			tentativas = 0;
			do {
				System.out.println("Digite a chave desejada");
				String chave = this.sc.next();
				Chave c = this.owner.getChave(chave);
				if (c == null) {
					System.out.println("Chave não encontrada");
					this.owner.adicionarRegistro(Calendar.getInstance().getTime().getDate(), Calendar.getInstance().getTime().getMonth(),
							Calendar.getInstance().getTime().getHours(), f, null, false, "Veiculo não existente");
				} else {
					int result = this.owner.cederChave(f, c);
					if (result == 0) {
						break;
					} else if (result == -1) {
						System.out.println("Usuario nao possui acesso a chave");
					} else if (result == -2) {
						System.out.println("Usuario já possui chave");
						break;
					} else if (result == -3) {
						System.out.println("Usuario Bloqueado");
						break;
					} else {
						System.out.println("Chave já alugada");
					}

				}
				tentativas++;
			} while (tentativas < 3);

			if (tentativas >= 3) {
				System.out.println("Usuario Bloqueado");
				this.owner.bloquearFuncionario(f);
			}
		} catch (Exception e) {

			System.out.println("Formato Incorreto de Preenchimento");
			this.sc.nextLine();
			return;
		}
	}

	private void exibeTelaDevolver() {
		try {
			cls();
			int tentativas = 0;
			Funcionario f = null;

			while (tentativas < 3) {
				System.out.println("Digite a sua matrícula");
				int matricula = this.sc.nextInt();
				f = this.owner.pegaFuncionario(matricula);

				if (f == null) {
					System.out.println("Funcionário não encontrado");
					this.owner.adicionarRegistro(Calendar.getInstance().getTime().getDate(), Calendar.getInstance().getTime().getMonth(),
							Calendar.getInstance().getTime().getHours(), f, null, false, "Devolucao : Matricula não existente");
				} else {
					break;
				}
				tentativas++;
			}
			if (tentativas >= 3) {
				System.out.println("Matriculas erradas muitas vezes");
				this.owner.adicionarRegistro(Calendar.getInstance().getTime().getDate(), Calendar.getInstance().getTime().getMonth(), Calendar.getInstance().getTime().getHours(),
						f, null, false, "Devolucao : Matricula não existente varias vezes");
				return;
			}

			Chave c = f.getChave();
			if (c == null) {
				System.out.println("Funcionario não possui chave");
				this.owner.adicionarRegistro(Calendar.getInstance().getTime().getDate(), Calendar.getInstance().getTime().getMonth(), Calendar.getInstance().getTime().getHours(),
						f, null, false, "Devolucao : Funcionario nao possui chave");
			} else {
				int result = this.owner.devolverChave(f, c);
				if (result == 0) {
					return;
				}
			}

		} catch (Exception e) {

			System.out.println("Formato Incorreto de Preenchimento");
			this.sc.nextLine();
			return;
		}
	}

	public static void cls() {
		try {
			Runtime.getRuntime().exec("clear");
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
}