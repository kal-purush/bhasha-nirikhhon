package br.com.brunopardini.TreinamentoGit.SecaoUm;

import java.util.Calendar;
import java.util.Date;

public class Transferencia {

	private int id;
	private Conta contaOrigem;
	private Conta contaDestino;
	private double valorTransferencia;
	public String banco;
	public Calendar dataTransferencia;
	
	public double saldoDaContaOrigemAntes;
	public double saldoDaContaOrigemDepois;
	
	public double saldoDaContaDestinoAntes;
	public double saldoDaContaDestinoDepois;
	
	public Transferencia(int id, Conta contaOrigem, Conta contaDestino, double transferencia, String banco) {
		this.id = id;
		this.contaOrigem = contaDestino;
		this.contaDestino = contaDestino;
		this.valorTransferencia = transferencia;
		saldoDaContaOrigemAntes = contaOrigem.getSaldo();
		saldoDaContaDestinoAntes = contaDestino.getSaldo();
		this.banco = banco;
	}
	
	public void registrarTransferencia() {
		dataTransferencia = construirDataTransferencia();
		saldoDaContaOrigemDepois = contaOrigem.getSaldo();
		saldoDaContaDestinoDepois = contaDestino.getSaldo();
	}
	
	private Calendar construirDataTransferencia() {
		Date data = new Date(System.currentTimeMillis());
		Calendar dataTransacao = Calendar.getInstance();
		dataTransacao.setTime(data);
		return dataTransacao;
	}

	public Conta getContaDestino() {
		return contaDestino;
	}

	public double getTransferencia() {
		return valorTransferencia;
	}
	
	public int getId() {
		return id;
	}
}