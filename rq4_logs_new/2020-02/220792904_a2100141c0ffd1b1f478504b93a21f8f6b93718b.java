package main.controller;

import javax.validation.ValidationException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import main.dto.LekarDTO;
import main.dto.OcenaKlinikeDTO;
import main.service.LekarService;
import main.service.OcenaKlinikeService;

@CrossOrigin
@RestController
@RequestMapping(value = "/ocenaKlinike")
public class OcenaKlinikeController {
	@Autowired
	private OcenaKlinikeService ocenaService;
	
	@PostMapping(value = "/dodaj", consumes = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("hasAuthority('PACIJENT')")
	public ResponseEntity<OcenaKlinikeDTO> dodajOcenu(@RequestBody OcenaKlinikeDTO ocenaDTO) {
		
		OcenaKlinikeDTO ocenadto = new OcenaKlinikeDTO();
		try {
			ocenadto = ocenaService.dodajOcenu(ocenaDTO);
			if (ocenadto ==null) {
				return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (ValidationException e) {
			return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		return new ResponseEntity<>(ocenadto, HttpStatus.OK);
	}
	

}