package br.com.caelum.viagens.voos.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import br.com.caelum.viagens.administrativo.model.Aeroporto;
import br.com.caelum.viagens.administrativo.model.Companhia;
import br.com.caelum.viagens.administrativo.model.Pais;

public class VooTests {
	
	private Companhia companhia;
	private br.com.caelum.viagens.administrativo.model.Rota rotaAtoB;
	private br.com.caelum.viagens.administrativo.model.Rota rotaBtoA;
	private br.com.caelum.viagens.administrativo.model.Rota rotaBtoC;
	private br.com.caelum.viagens.administrativo.model.Rota rotaCtoU;
	private br.com.caelum.viagens.administrativo.model.Rota rotaBtoU;
	private br.com.caelum.viagens.administrativo.model.Rota rotaUtoA;
	
	@BeforeEach
	public void setUp() {
		Aeroporto aeroportoA = new Aeroporto("AeroportoA", new Pais("Argentina"));
		Aeroporto aeroportoB = new Aeroporto("AeroportoB", new Pais("Brasil"));
		Aeroporto aeroportoC = new Aeroporto("AeroportoC", new Pais("Chile"));
		Aeroporto aeroportoU = new Aeroporto("AeroportoU", new Pais("Uruguai"));
		
		this.rotaAtoB = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoA, aeroportoB, 120);
		this.rotaBtoA = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoB, aeroportoA, 120);
		this.rotaBtoC = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoB, aeroportoC, 120);
		this.rotaCtoU = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoC, aeroportoU, 120);
		this.rotaBtoU = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoB, aeroportoU, 120);
		this.rotaUtoA = new br.com.caelum.viagens.administrativo.model.Rota(aeroportoU, aeroportoA, 120);
		
		this.companhia = new Companhia("Companhia", new Pais("Argentina"));
	}
	@Test
	public void deveInstanciarVooComApenasUmaRotaValida() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.add(rotaVooAtoB);
		
		Voo voo = new Voo(rotas , this.companhia, 100);
		assertThat(voo).isNotNull();
	}
	
	@Test
	public void deveInstanciarVooComRotasEmSequenciaLogica() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoC = new Rota(rotaBtoC, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooCtoU = new Rota(rotaCtoU);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.add(rotaVooAtoB);
		rotas.add(rotaVooBtoC);
		rotas.add(rotaVooCtoU);
		
		Voo voo = new Voo(rotas , this.companhia, 100);
		assertThat(voo).isNotNull();
	}
	
	@Test
	public void deveRetornarRotasEmSequenciaLogica() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoC = new Rota(rotaBtoC, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooCtoU = new Rota(rotaCtoU);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.addAll(List.of(rotaVooAtoB, rotaVooBtoC, rotaVooCtoU));
		
		Voo voo = new Voo(rotas , this.companhia, 100);
			
		List<Rota> rotasEmSequenciaLogica = voo.getRotasEmSequenciaLogica();
		
		assertThat(rotasEmSequenciaLogica).isNotEmpty();
		assertThat(rotasEmSequenciaLogica.size()).isEqualTo(3);
		assertThat(rotasEmSequenciaLogica.get(0)).isEqualTo(rotaVooAtoB);
		assertThat(rotasEmSequenciaLogica.get(1)).isEqualTo(rotaVooBtoC);
		assertThat(rotasEmSequenciaLogica.get(2)).isEqualTo(rotaVooCtoU);
	}
	
	
	@Test
	public void naoDeveInstanciarVooComRotasComDoisCandidatosADestino() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoC = new Rota(rotaBtoC, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoU = new Rota(rotaBtoU);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.addAll(Set.of(rotaVooAtoB, rotaVooBtoC, rotaVooBtoU));
		
		Exception exception = assertThrows(IllegalArgumentException.class, () -> {
			new Voo(rotas, this.companhia, 100);
		});
		
	    assertThat(exception.getMessage()).isEqualTo("rotas devem possuir uma sequência lógica.");
	}
	
	@Test
	public void naoDeveInstanciarVooComRotasComDoisCandidatosAOrigem() {
		
		Rota rotaVooBtoU = new Rota(rotaBtoU, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooCtoU = new Rota(rotaCtoU, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooUtoA = new Rota(rotaUtoA);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.addAll(Set.of(rotaVooBtoU, rotaVooCtoU, rotaVooUtoA));
		
		Exception exception = assertThrows(IllegalArgumentException.class, () -> {
			new Voo(rotas, this.companhia, 100);
		});
	 
	    assertThat(exception.getMessage()).isEqualTo("rotas devem possuir uma sequência lógica.");
	}
	
	@Test
	public void naoDeveInstanciarVooComRotasComOrigemIgualADestino() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoA = new Rota(rotaBtoA);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.addAll(Set.of(rotaVooAtoB, rotaVooBtoA));
		
		Exception exception = assertThrows(IllegalArgumentException.class, () -> {
			new Voo(rotas, this.companhia, 100);
		});
		
	    assertThat(exception.getMessage()).isEqualTo("rotas devem possuir uma sequência lógica.");
	}
	
	@Test
	public void naoDeveInstanciarVooComRotasComMaisDeUmaPernaFinal() {
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoC = new Rota(rotaBtoC);
		Rota rotaVooCtoU = new Rota(rotaCtoU);
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.addAll(Set.of(rotaVooAtoB, rotaVooBtoC, rotaVooCtoU));
		
		Exception exception = assertThrows(IllegalArgumentException.class, () -> {
			new Voo(rotas, this.companhia, 100);
		});
		
	    assertThat(exception.getMessage()).isEqualTo("rotas deve conter uma única perna final.");
	}
	
	@Test
	public void naoDeveInstanciarVooComRotasSemPernaFinal() {
		
		Rota rotaVooAtoB = new Rota(rotaAtoB, new Parada(40, TipoParada.ESCALA));
		Rota rotaVooBtoC = new Rota(rotaBtoC, new Parada(40, TipoParada.ESCALA));
		
		Set<Rota> rotas = new HashSet<Rota>();
		rotas.add(rotaVooAtoB);
		rotas.add(rotaVooBtoC);
		
		Exception exception = assertThrows(IllegalArgumentException.class, () -> {
			new Voo(rotas, this.companhia, 100);
		});
		
		assertThat(exception.getMessage()).isEqualTo("rotas deve conter uma única perna final.");
	}
	
}