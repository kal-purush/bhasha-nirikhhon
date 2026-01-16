package br.com.caelum.viagens.voos.validator;

import java.util.Optional;

import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import br.com.caelum.viagens.voos.controller.dto.input.NewPassagemInputDto;
import br.com.caelum.viagens.voos.model.Passagem;
import br.com.caelum.viagens.voos.repository.PassagemRepository;

public class AssentoDisponivelValidator implements Validator {
	
	private PassagemRepository passagemRepository;
	
	public AssentoDisponivelValidator(PassagemRepository passagemRepository) {
		this.passagemRepository = passagemRepository;
	}
	
	@Override
	public boolean supports(Class<?> clazz) {
		return NewPassagemInputDto.class.isAssignableFrom(clazz);
	}

	@Override
	public void validate(Object target, Errors errors) {
		NewPassagemInputDto newPassagemDto = (NewPassagemInputDto) target;
		
		if(!errors.hasErrors()) {
			Optional<Passagem> passagem = passagemRepository.findByAssentoId(newPassagemDto.getAssentoId());
		
			if(passagem.isPresent()) {
				errors.rejectValue("assentoId", null, "assento não disponível.");
			}
		}
	}

}