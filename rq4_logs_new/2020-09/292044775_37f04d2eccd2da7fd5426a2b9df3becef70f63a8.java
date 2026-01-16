package poo;

public class Carro {
	public String modelo;
	public String cor;
	public int quantidadeLugares;
	

	public Carro(String modelo, String cor, int quantidadeLugares) {
		this.modelo = modelo;
		this.cor = cor;
		this.quantidadeLugares = quantidadeLugares;
	}
	
	public Carro(String modelo, int quantidadeLugares) {
		this.modelo = modelo;
		this.quantidadeLugares = quantidadeLugares;
	}
	
	public void imprimirDados() {
		System.out.println("Modelo: " + modelo);
		System.out.println("Cor: " + cor);
		System.out.println("Lugares: " + quantidadeLugares);		
	}
	
	public String retornarDados() {
		return "Modelo: " + modelo + "\nCor: " + cor + "\nLugares: " + quantidadeLugares;		
	}
	
}