package br.com.caelum.viagens.voos.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.caelum.viagens.voos.controller.dto.input.NewPassagemInputDto;
import br.com.caelum.viagens.voos.model.Passagem;
import br.com.caelum.viagens.voos.repository.PassagemRepository;
import br.com.caelum.viagens.voos.repository.VooRepository;

@RequestMapping("/passagens")
@RestController
public class PassagensController {
	
	@Autowired
	private VooRepository vooRepository;
	
	@Autowired
	private PassagemRepository passagemRepository;
	
	@PostMapping
	public ResponseEntity<?> cadastro(@Valid @RequestBody NewPassagemInputDto newPassagemDto) {
		Passagem passagem = newPassagemDto.toModel(this.vooRepository);
		this.passagemRepository.save(passagem);
		return ResponseEntity.ok().build();
	}

}