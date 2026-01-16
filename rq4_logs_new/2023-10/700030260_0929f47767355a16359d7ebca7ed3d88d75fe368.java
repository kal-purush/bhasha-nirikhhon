package br.edu.ifsp.projeto_passagem_aerea.application.model;

public class AssentoDTO {

	private int voo;
	
	private Integer fileira;
	
	private Integer poltrona;
	
	public AssentoDTO(int voo, Integer fileira, Integer poltrona) {
		this.voo = voo;
		this.fileira = fileira;
		this.poltrona = poltrona;
	}

	public int getVoo() {
		return voo;
	}

	public void setVoo(int voo) {
		this.voo = voo;
	}

	public Integer getFileira() {
		return fileira;
	}

	public void setFileira(Integer fileira) {
		this.fileira = fileira;
	}

	public Integer getPoltrona() {
		return poltrona;
	}

	public void setPoltrona(Integer poltrona) {
		this.poltrona = poltrona;
	}
	
}