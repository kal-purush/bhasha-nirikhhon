package src.model;

public class ItemChaveiro {

	private String nomeArquivo;
	private String chaveArquivo;

	public ItemChaveiro(String nomeArquivo, String chaveArquivo) {
		this.nomeArquivo = nomeArquivo;
		this.chaveArquivo = chaveArquivo;
	}

	public String getNomeArquivo() {
		return this.nomeArquivo;
	}

	public void setNomeArquivo(String nomeArquivo) {
		this.nomeArquivo = nomeArquivo;
	}

	public String getChaveArquivo() {
		return this.chaveArquivo;
	}

	public void setChaveArquivo(String chaveArquivo) {
		this.chaveArquivo = chaveArquivo;
	}

}