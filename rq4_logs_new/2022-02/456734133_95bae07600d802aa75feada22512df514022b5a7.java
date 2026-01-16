package br.edu.infnet.testes;

import br.edu.infnet.dominio.Contato;

public class ContatoTeste {

	public static void main(String[] args) {
		
		Contato contato = new Contato();
		contato.setEmail("contato@contato.com");
		contato.setTelefone("234987234");
		System.out.println(contato);
	}
}