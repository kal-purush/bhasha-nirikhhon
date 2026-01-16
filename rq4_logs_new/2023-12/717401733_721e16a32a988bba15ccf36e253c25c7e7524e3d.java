package com.proyecto.integrador.hotel.libertador.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.proyecto.integrador.hotel.libertador.models.entity.Informes;
import com.proyecto.integrador.hotel.libertador.models.entity.Usuario;
import com.proyecto.integrador.hotel.libertador.models.service.IInformesService;
import com.proyecto.integrador.hotel.libertador.models.service.IUsuarioService;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;

@CrossOrigin(origins= {"http://localhost:5173"})
@RestController
@RequestMapping("/api")
public class InformesRestController {
	@Autowired
	private IInformesService informesService;
	
	@GetMapping("/informes")
	public List<Informes> index() {
		return informesService.findAll();
	}
	
	@GetMapping("/informes/page/{page}")
	public Page<Informes> index(@PathVariable Integer page) {
	    Pageable pageable = PageRequest.of(page, 10);
	    return informesService.findAll(pageable);
	}

	@GetMapping("/informes/{id}")
	public ResponseEntity<?> show(@PathVariable Long id) {
		Informes informes = null;
		Map<String, Object> response = new HashMap();

		try {
			informes = informesService.findById(id);

		} catch (DataAccessException e) {
			response.put("mensaje", "Error al realizar la consulta en la base de datos");
			response.put("error", e.getMessage().concat(":").concat(e.getMostSpecificCause().getMessage()));
			return new ResponseEntity<Map<String, Object>>(response, HttpStatus.INTERNAL_SERVER_ERROR);
		}

		if (informes == null) {
			response.put("mensaje", "El informe ID: ".concat(id.toString().concat(" no existe en la base de datos")));
			return new ResponseEntity<Map<String, Object>>(response, HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Informes>(informes, HttpStatus.OK);
	}

	@PostMapping("/informes")
	public ResponseEntity<?> create(@Valid @RequestBody Informes informes, BindingResult result) {
		Informes nuevoInforme=null;
		Map<String, Object> response = new HashMap();
		
		if(result.hasErrors()) {
			List<String> errors=result.getFieldErrors()
					.stream()
					.map(err->"El campo '"+err.getField()+"' "+err.getDefaultMessage())
					.collect(Collectors.toList());
			
			response.put("errors", errors);
			return new ResponseEntity<Map<String, Object>>(response, HttpStatus.BAD_REQUEST);
		}	
		
		try {
			nuevoInforme=informesService.save(informes);
			
		}catch (DataAccessException e) {
			response.put("mensaje", "Error al realizar el insert en la base de datos");
			response.put("error", e.getMessage().concat(":").concat(e.getMostSpecificCause().getMessage()));
			if (e.getCause() instanceof ConstraintViolationException) {
	            return new ResponseEntity<Map<String, Object>>(response, HttpStatus.CONFLICT);
	        }

	        return new ResponseEntity<Map<String, Object>>(response, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		response.put("mensaje", "El informe se creo con exito");
		response.put("informe", nuevoInforme);
		return new ResponseEntity<Map<String,Object>>(response ,HttpStatus.CREATED);
	}
	
}