package hyve.petshow.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import hyve.petshow.controller.converter.ContaConverter;
import hyve.petshow.controller.representation.ContaRepresentation;
import hyve.petshow.service.port.GenericContaService;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.info.Info;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/conta")
@OpenAPIDefinition(info = @Info(title = "API de consulta a contas", description = "API utilizada para a consulta de contas, sem discernimento se é cliente ou prestador"))
public class ContaController {
	@Autowired
	private GenericContaService service;
	@Autowired
	private ContaConverter converter;

	@Operation(summary = "Busca conta por id.")
	@GetMapping("/{id}")
	public ResponseEntity<ContaRepresentation> buscarContaPorId(
			@Parameter(description = "Id da conta.") @PathVariable Long id) throws Exception {
		var cliente = service.buscarPorId(id);
		var representation = converter.toRepresentation(cliente);

		return ResponseEntity.status(HttpStatus.OK).body(representation);
	}

}