package pe.edu.ceid.simi.operative.web.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin(origins = "localhost:4200")
@RestController
@RequestMapping({ "/api/v1/sistema" })
public class SistemaController {

	
	@SuppressWarnings("rawtypes")
	@GetMapping(path = {"verificarSistemaMatricula"})
	public ResponseEntity verificarSistemaMatricula() {
		return new ResponseEntity(HttpStatus.OK);
	}
}