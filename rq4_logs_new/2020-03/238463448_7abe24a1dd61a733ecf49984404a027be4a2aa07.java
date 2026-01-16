package br.com.caelum.viagens.voos.controller.dto.input;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import javax.validation.constraints.Future;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;

import br.com.caelum.viagens.voos.model.Passagem;
import br.com.caelum.viagens.voos.model.Voo;

public class NewPassagensInputDto {
	
	@JsonFormat(shape = Shape.STRING, pattern = "dd/MM/yyyy kk:mm")
	@Future
	@NotNull
	private LocalDateTime dataEHoraDePartida;
	
	@JsonFormat(shape= Shape.NUMBER_FLOAT)
	@Positive
	@NotNull
	private BigDecimal valor;
	
	public LocalDateTime getDataEHoraDePartida() {
		return dataEHoraDePartida;
	}
	
	public void setDataEHoraDePartida(LocalDateTime dataEHoraDePartida) {
		this.dataEHoraDePartida = dataEHoraDePartida;
	}

	public void setValor(BigDecimal valor) {
		this.valor = valor;
	}

	public Voo toModel(Voo voo) {		
		voo.getAeronave().getAssentos().forEach(assento -> {
			voo.getPassagens().add(new Passagem(voo, this.dataEHoraDePartida, this.valor, assento));
		});
		return voo;
	}
	
}