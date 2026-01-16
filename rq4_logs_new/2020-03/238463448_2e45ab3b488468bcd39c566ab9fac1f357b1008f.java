package br.com.caelum.viagens.voos.validator;

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import br.com.caelum.viagens.voos.controller.dto.input.NewPassagemInputDto;
import br.com.caelum.viagens.voos.model.Voo;
import br.com.caelum.viagens.voos.repository.VooRepository;

public class AssentoExistenteNoVooValidator implements Validator {

	private VooRepository vooRepository;
	
	public AssentoExistenteNoVooValidator(VooRepository vooRepository) {
		this.vooRepository = vooRepository;
	}

	@Override
	public boolean supports(Class<?> clazz) {
		return NewPassagemInputDto.class.isAssignableFrom(clazz);
	}

	@Override
	public void validate(Object target, Errors errors) {
		NewPassagemInputDto newPassagemDto = (NewPassagemInputDto) target;

		if (!errors.hasErrors()) {
			Voo vooBuscado = vooRepository.findById(newPassagemDto.getVooId()).get();
			
			Set<Long> assentosIds = vooBuscado.getAeronave().getAssentos().stream()
				.map(a -> a.getId()).collect(Collectors.toSet());
			
			if (!assentosIds.contains(newPassagemDto.getAssentoId())) {
				errors.rejectValue("assentoId", null, "assento não existe no voo.");
			}
		}
	}

}