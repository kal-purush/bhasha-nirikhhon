package br.com.caelum.viagens.voos.validator;

import java.util.Optional;

import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import br.com.caelum.viagens.voos.controller.dto.input.NewPassagemInputDto;
import br.com.caelum.viagens.voos.model.Voo;
import br.com.caelum.viagens.voos.repository.VooRepository;

public class VooExistenteValidator implements Validator{
	
	private VooRepository vooRepository;
	
	public VooExistenteValidator(VooRepository vooRepository) {
		this.vooRepository = vooRepository;
	}
	
	@Override
	public boolean supports(Class<?> clazz) {
		return NewPassagemInputDto.class.isAssignableFrom(clazz);
	}

	@Override
	public void validate(Object target, Errors errors) {
		NewPassagemInputDto newPassagemDto = (NewPassagemInputDto) target;
		
		if(!errors.hasErrors()) {
			Optional<Voo> vooBuscado = this.vooRepository.findById(newPassagemDto.getVooId());
			
			if(!vooBuscado.isPresent()) {
				errors.rejectValue("vooId", null, "vooId não existe no sistema.");
			}
		}
	}

}