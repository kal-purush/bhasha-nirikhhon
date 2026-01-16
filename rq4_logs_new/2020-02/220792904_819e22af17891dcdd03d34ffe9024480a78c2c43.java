package main.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.validation.ValidationException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import main.dto.ZahtevZaOdmorDTO;
import main.model.ZahtevZaOdmor;
import main.service.ZahtevZaOdmorService;

@CrossOrigin
@RestController
@RequestMapping(value = "/zahtevZaOdmor")
public class ZahtevZaOdmorController {
	@Autowired
	private ZahtevZaOdmorService zahtevZaOdmorService;
	
	@PostMapping(value = "/zatrazi", consumes = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("hasAuthority('LEKAR')")
	public ResponseEntity<ZahtevZaOdmorDTO> zatrazi(@RequestBody ZahtevZaOdmorDTO zahtevZaOdmorDTO) {
		
		ZahtevZaOdmorDTO zzodto = new ZahtevZaOdmorDTO();
		try {
			zzodto = zahtevZaOdmorService.zatrazi(zahtevZaOdmorDTO);
			if (zzodto ==null) {
				return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (ValidationException e) {
			return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		return new ResponseEntity<>(zzodto, HttpStatus.OK);
	}
	
	@PutMapping(value = "/odobri", consumes = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("hasAuthority('ADMIN_KLINIKE')")
	public ResponseEntity<?> odobri(@RequestBody ZahtevZaOdmorDTO zahtevZaOdmorDTO) {
		ZahtevZaOdmor zzo = new ZahtevZaOdmor();
		try {
			zzo = zahtevZaOdmorService.odobri(zahtevZaOdmorDTO);
			if (zzo ==null) {
				return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (ValidationException e) {
			return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		

		return new ResponseEntity<>(true, HttpStatus.OK);
	}
	
	@PutMapping(value = "/odbij", consumes = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("hasAuthority('ADMIN_KLINIKE')")
	public ResponseEntity<?> odbij(@RequestBody ZahtevZaOdmorDTO zahtevZaOdmorDTO) {
		ZahtevZaOdmor zzo = new ZahtevZaOdmor();
		try {
			zzo = zahtevZaOdmorService.odbij(zahtevZaOdmorDTO);
			if (zzo ==null) {
				return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (ValidationException e) {
			return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		

		return new ResponseEntity<>(true, HttpStatus.OK);
	}
}