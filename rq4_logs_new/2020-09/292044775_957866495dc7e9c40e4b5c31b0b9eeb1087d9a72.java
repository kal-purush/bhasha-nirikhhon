package org.etec;

// public, private, protected e default
// default - visibilidade a nível de pacote

// classe abstract = reaproveitar codigo através da herança e do polimorfismo

public abstract class Produto {
	protected String nome;
	public double preco;
	
	public Produto(String nome, double preco) {
		super();
		this.nome = nome;
		this.preco = preco;
	}
	
	public void setName(String nome) {
		this.nome = nome;
	}
	
	public String getName() {
		return nome;
	}
	
	public abstract void resume();
	
}