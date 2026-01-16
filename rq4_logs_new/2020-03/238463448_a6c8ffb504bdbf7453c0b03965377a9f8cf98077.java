package br.com.caelum.viagens.aeronaves.controller;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import br.com.caelum.viagens.aeronaves.controller.dto.input.NewAeronaveInputDto;
import br.com.caelum.viagens.aeronaves.controller.dto.output.AeronaveOutputDto;
import br.com.caelum.viagens.aeronaves.model.Aeronave;
import br.com.caelum.viagens.support.IfResourceIsFound;
import io.github.asouza.FormFlow;

@RestController
@RequestMapping("/aeronaves")
public class AeronavesController {
	
	@Autowired
	private FormFlow<Aeronave> flow;
	
	@PostMapping
	public ResponseEntity<Map<String, Object>> cadastro(@Valid @RequestBody NewAeronaveInputDto newAeronaveDto, UriComponentsBuilder uriBuilder) {
		Aeronave aeronave = flow.save(newAeronaveDto).getEntity();
		
		URI location = uriBuilder.path("/aeronaves/{id}").buildAndExpand(aeronave.getId()).toUri();
		
		return ResponseEntity.created(location).body(AeronaveOutputDto.criado(aeronave));
	}
	
	@GetMapping("/{id}")
	public ResponseEntity<Map<String, Object>> detalhes(@PathVariable("id") Optional<Aeronave> aeronave) {
		return ResponseEntity.ok(AeronaveOutputDto.detalhes(IfResourceIsFound.of(aeronave)));
	}
}