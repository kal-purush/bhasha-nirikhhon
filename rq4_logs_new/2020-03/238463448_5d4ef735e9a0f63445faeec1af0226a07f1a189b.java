package br.com.caelum.viagens.voos.model;

import javax.validation.constraints.NotNull;

import org.springframework.util.Assert;

import br.com.caelum.viagens.administrativo.model.Aeroporto;
import br.com.caelum.viagens.support.PossuiOrigemEDestino;

public class RotaSemVoo implements PossuiOrigemEDestino {
	
	@NotNull
	private br.com.caelum.viagens.administrativo.model.Rota rota;
	
	private Parada parada;

	public RotaSemVoo(@NotNull br.com.caelum.viagens.administrativo.model.Rota rota) {
		this(rota, null);
	}
	
	public RotaSemVoo(@NotNull br.com.caelum.viagens.administrativo.model.Rota rota, Parada parada) {
		Assert.notNull(rota, "A rota não pode ser nula.");
		this.rota = rota;
		this.parada = parada;
	}
	
	public boolean isPernaFinal() {
		return this.parada == null;
	}
	
	public Aeroporto getDestino() {
		return rota.getDestino();
	}
	
	public Aeroporto getOrigem() {
		return rota.getOrigem();
	}
	
	public Parada getParada() {
		return parada;
	}

	public void setParada(Parada parada) {
		this.parada = parada;
	}
	
	public br.com.caelum.viagens.administrativo.model.Rota getRota() {
		return rota;
	}
}