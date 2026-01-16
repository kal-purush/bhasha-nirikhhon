package br.com.caelum.viagens.voos.validator;

import java.util.Optional;

import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import br.com.caelum.viagens.aeronaves.model.Aeronave;
import br.com.caelum.viagens.aeronaves.repository.AeronaveRepository;
import br.com.caelum.viagens.voos.controller.dto.input.NewVooInputDto;

public class AeronaveExistenteValidator implements Validator{
	
	private AeronaveRepository aeronaveRepository;
	
	public AeronaveExistenteValidator(AeronaveRepository aeronaveRepository) {
		this.aeronaveRepository = aeronaveRepository;
	}
	
	@Override
	public boolean supports(Class<?> clazz) {
		return NewVooInputDto.class.isAssignableFrom(clazz);
	}

	@Override
	public void validate(Object target, Errors errors) {
		NewVooInputDto newVooDto = (NewVooInputDto) target;
		
		if(!errors.hasErrors()) {
			Optional<Aeronave> aeronaveBuscada = 
					this.aeronaveRepository.findById(newVooDto.getAeronaveId());
			
			if(!aeronaveBuscada.isPresent()) {
				errors.rejectValue("aeronaveId", null, "aeronave não existe no sistema.");
			}
		}
		
	}

}