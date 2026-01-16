package com.moto.service.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.moto.service.entity.Moto;
import com.moto.service.servicio.MotoService;

@RestController
@RequestMapping("/moto")
public class MotoController {
	
	
	@Autowired
	MotoService motoService;
	
	
	@PostMapping("/guardar")
	public ResponseEntity<Moto> save(@RequestBody Moto moto) {
		return new ResponseEntity<Moto>(motoService.save(moto),HttpStatus.OK);
	}
	
	@GetMapping("/listar")
	public ResponseEntity<List<Moto>> findAll(){
		return new ResponseEntity<List<Moto>>(motoService.findAll(),HttpStatus.OK);
	}
	
	@GetMapping("/moto/{id}")
	public ResponseEntity<Moto> findById(@PathVariable int id){
		return new ResponseEntity<>(motoService.findById(id),HttpStatus.OK);
	}
	
	
	

}