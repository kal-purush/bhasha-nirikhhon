package br.com.empresa.banco;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import br.com.empresa.banco.conta.Conta;

public class Banco {

	private List<Conta> contas = new LinkedList<>();
    Map<String, Conta> indexadoPorNome  = new HashMap<>();
	public void adiciona(Conta conta) {
		contas.add(conta);
        indexadoPorNome.put(conta.getNome(), conta);

	}

	public Conta pega(int posicao) {

		return contas.get(posicao);
	}

	public int pegaQuantidadeDeContas() {

		return contas.size();
	}

	public Conta buscaPorNome(String nome) {
        return indexadoPorNome.get(nome);

	}

}