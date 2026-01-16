package br.net.eia.movimento;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import br.net.eia.entities.BaseEntity;
import br.net.eia.notafiscal.ide.ModNFe;

@Entity
public class NF extends BaseEntity{
	@Temporal(TemporalType.TIMESTAMP)
	private Date emissao;
	@Enumerated(EnumType.STRING)
	private ModNFe mod;
	private String serie;
	private String numero;
	private String chave;
	
	public NF(){
		
	}

	public Date getEmissao() {
		return emissao;
	}

	public void setEmissao(Date emissao) {
		this.emissao = emissao;
	}

	public ModNFe getMod() {
		return mod;
	}

	public void setMod(ModNFe mod) {
		this.mod = mod;
	}

	public String getSerie() {
		return serie;
	}

	public void setSerie(String serie) {
		this.serie = serie;
	}

	public String getNumero() {
		return numero;
	}

	public void setNumero(String numero) {
		this.numero = numero;
	}

	public String getChave() {
		return chave;
	}

	public void setChave(String chave) {
		this.chave = chave;
	}
}