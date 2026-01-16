package br.com.monetizacao.entidades;


public class Conta {

	private int numero;
	private double saldo;
	private Cliente cliente;

	public Conta(int numero, double saldo, Cliente cliente) {
		this.numero = numero;
		this.saldo = saldo;
		this.cliente = cliente;
	}

	public int getNumero() {
		return numero;
	}

	public double getSaldo() {
		return saldo;
	}

	public Cliente getCliente() {
		return cliente;
	}

	public boolean depositar(double valor) {
		if (valor > 0) {
			this.saldo = this.saldo + valor;
			return true;
		} else {
			System.out.println("O Valor do deposito � inv�lido!!!");
			return false;
		}
	}

	public double transferePara(Conta contaDoCliente, double valorDoDeposito, Conta contaDoVendedor) {
		if (valorDoDeposito < contaDoCliente.saldo && valorDoDeposito > 0) {
			contaDoCliente.saldo -= valorDoDeposito;
			return contaDoVendedor.saldo += valorDoDeposito;
		}else {
			System.out.println("Valor do deposito incorreto.");
			return 0;
		}

	}

	@Override
	public String toString() {
		return "N�mero da conta " + getNumero() + ", Saldo: " + getSaldo();
	}

}