package br.com.caelum.viagens.voos.controller.dto.output;

import java.util.Map;

import br.com.caelum.viagens.voos.model.Passagem;
import io.github.asouza.DataView;

public class PassagemOutputDto {
	
	public static Map<String, Object> criado(Passagem passagem) {
		return DataView.of(passagem)
				.add(Passagem::getId)
				.add("voo", Passagem::getVoo)
				.addDate("dataEHoraDePartida", p -> p.getDataEHoraDePartida(), "dd/MM/yyyy HH:mm:ss")
				.add("valor", Passagem::getValor)
				.build();
	}
	
	public static Map<String, Object> detalhes(Passagem passagem) {
		return DataView.of(passagem)
				.add(Passagem::getId)
				.add("voo", Passagem::getVoo)
				.addDate("dataEHoraDePartida", p -> p.getDataEHoraDePartida(), "dd/MM/yyyy HH:mm:ss")
				.add("valor", Passagem::getValor)
				.build();
	}
}