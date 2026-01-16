package Banco;

public class BancoTerminal {
	
	public static void main(String[] args) {
		
		double saldo = 25;
		double valorSolicitado = 18;
		
		// Verifica se o saldo é suficinete para o saque
		
		if(saldo >= valorSolicitado) {
			
			saldo -= valorSolicitado;
			System.out.println("Saque realizado com sucesso!");
			System.out.println("Saldo atual: " + saldo);
			
		} else {
			
			System.out.println("Saldo insuficiente");
			System.out.println("Saldo atual: " + saldo);
		}
		
		// Segunda execução
		
		double saldo2 = 15;
		double valorSolicitado2 = 22;
		
		if(saldo2 >= valorSolicitado2) {
			
			saldo2 -= valorSolicitado2;
			System.out.println("Saque realizado com sucesso!");
			System.out.println("Saldo atual: " + saldo2);
			
		} else {
			
			System.out.println("Saldo insuficiente");
			System.out.println("Saldo atual: " + saldo2);
		}
		
		
	}
	
}