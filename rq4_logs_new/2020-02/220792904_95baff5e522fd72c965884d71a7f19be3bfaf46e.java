package main.dto;

public class PromenaLozinkeDTO {
	
	private String staraLozinka;
	private String novaLozinka;
	private String ponovljenaLozinka;

	public PromenaLozinkeDTO() {
		
	}

	public PromenaLozinkeDTO(String staraLozinka, String novaLozinka, String ponovljenaLozinka) {
		super();
		this.staraLozinka = staraLozinka;
		this.novaLozinka = novaLozinka;
		this.ponovljenaLozinka = ponovljenaLozinka;
	}

	public String getStaraLozinka() {
		return staraLozinka;
	}

	public void setStaraLozinka(String staraLozinka) {
		this.staraLozinka = staraLozinka;
	}

	public String getNovaLozinka() {
		return novaLozinka;
	}

	public void setNovaLozinka(String novaLozinka) {
		this.novaLozinka = novaLozinka;
	}

	public String getPonovljenaLozinka() {
		return ponovljenaLozinka;
	}

	public void setPonovljenaLozinka(String ponovljenaLozinka) {
		this.ponovljenaLozinka = ponovljenaLozinka;
	}
}