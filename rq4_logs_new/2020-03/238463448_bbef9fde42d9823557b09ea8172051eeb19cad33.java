package br.com.caelum.viagens.voos.validator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.validation.BeanPropertyBindingResult;

import br.com.caelum.viagens.administrativo.model.Aeroporto;
import br.com.caelum.viagens.administrativo.model.Companhia;
import br.com.caelum.viagens.administrativo.model.Pais;
import br.com.caelum.viagens.administrativo.model.Rota;
import br.com.caelum.viagens.voos.controller.dto.input.NewPassagemInputDto;
import br.com.caelum.viagens.voos.model.RotaSemVoo;
import br.com.caelum.viagens.voos.model.Voo;
import br.com.caelum.viagens.voos.repository.VooRepository;

public class VooExistenteValidatorTests {
	
	private VooExistenteValidator validator;
	private VooRepository vooRepository;
	

	@BeforeEach
	public void setUp() {
		this.vooRepository = Mockito.mock(VooRepository.class);
		
		Pais argentina = new Pais("Argentina");
		Pais brasil = new Pais("Brasil");
		
		Rota rota = new Rota(new Aeroporto("AeroportoA", argentina), new Aeroporto("AeroportoB", brasil), 90);
		Set<RotaSemVoo> rotas = new HashSet<>();
		rotas.add(new RotaSemVoo(rota));
		
		Voo voo = new Voo(rotas , new Companhia("CompanhiaA", argentina), 90);
				
		when(this.vooRepository.findById(1L)).thenReturn(Optional.of(voo));
		when(this.vooRepository.findById(20L)).thenReturn(Optional.empty());
		
		this.validator = new VooExistenteValidator(vooRepository);
	}
	
	@Test
	public void deveDetectarErroQuandoCadastrarPassagemComVooQueNaoExiste() {
		NewPassagemInputDto newPassagemDto = new NewPassagemInputDto();
		newPassagemDto.setVooId(20L);
		
		BeanPropertyBindingResult result = new BeanPropertyBindingResult(newPassagemDto, "newPassagenDto");
		validator.validate(newPassagemDto, result);
		
		assertThat(result.getFieldErrors()).isNotEmpty();
		assertThat(result.getFieldErrors().get(0).getField()).isEqualTo("vooId");
		assertThat(result.getFieldErrors().get(0).getDefaultMessage())
			.isEqualTo("vooId não existe no sistema.");
	}
	
	@Test
	public void naoDeveDetectarErroQuandoCadastrarVooComCompanhiaQueExiste() {
		NewPassagemInputDto newPassagemDto = new NewPassagemInputDto();
		newPassagemDto.setVooId(1L);
		
		BeanPropertyBindingResult result = new BeanPropertyBindingResult(newPassagemDto, "newPassagenDto");
		validator.validate(newPassagemDto, result);
		
		assertThat(result.getFieldErrors()).isEmpty();
	}
}