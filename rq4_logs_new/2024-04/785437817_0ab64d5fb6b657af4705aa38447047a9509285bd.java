package com.senac.CondoConnect.controller;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.senac.CondoConnect.Model.AssembleiaModel;
import com.senac.CondoConnect.dtos.AssembleiaRecord;
import com.senac.CondoConnect.service.AssembleiaService;

import jakarta.validation.Valid;

@RestController
public class AssembleiaController {

	@Autowired
	AssembleiaService assembleiaservice; 
	
	@PostMapping(value ="/newassembleia") //retorna 201
	public ResponseEntity<Object> saveAssembleia(@RequestBody @Valid AssembleiaRecord assembleiadto) {
		
		var assembleiamodel = new AssembleiaModel();
		BeanUtils.copyProperties(assembleiadto, assembleiamodel);
		return ResponseEntity.status(HttpStatus.CREATED).body(assembleiaservice.save(assembleiamodel));
	}
	
	@GetMapping(value = "/assembleia")
	public ResponseEntity<List<AssembleiaModel>> getAssembleia(){
		List<AssembleiaModel> assembleia = assembleiaservice.findAll();
		
		if (assembleia.isEmpty()) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(assembleia);
			
		}
			return ResponseEntity.status(HttpStatus.OK).body(assembleia);
	}
	
	@GetMapping(value ="/assembleia/{id}")
	public ResponseEntity<Object> getAssembleiaDetails(@PathVariable("id") UUID id) {
		Optional<AssembleiaModel> assembleia = assembleiaservice.findById(id);
		
		if(!assembleia.isPresent()) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body("assembleia not found.");
		}
		return ResponseEntity.status(HttpStatus.OK).body(assembleia.get());
	}
	
	@DeleteMapping(value = "/deleteassembleia/{id}")
	public ResponseEntity<Object> deleteAssembleia(@PathVariable("id") UUID id ){
		Optional<AssembleiaModel> blogappModelOptional = assembleiaservice.findById(id);
		
		if(!blogappModelOptional.isPresent()) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body("assembleia not found.");
		}
		assembleiaservice.delete(blogappModelOptional.get());
		return ResponseEntity.status(HttpStatus.OK).body("Deleted sucefully");
	}
	
	@PutMapping(value ="/putassembleia/{id}")
	public ResponseEntity<Object> putAssembleia(@RequestBody @Valid AssembleiaRecord assembleiadto,@PathVariable("id") UUID id){
		Optional<AssembleiaModel> blogappModelOptional = assembleiaservice.findById(id);

		if(!blogappModelOptional.isPresent()) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body("assembleia not found.");
		}
		var assembleiaModel = new AssembleiaModel();
		BeanUtils.copyProperties(assembleiadto, assembleiaModel);
		assembleiaModel.setId(blogappModelOptional.get().getId());
		return ResponseEntity.status(HttpStatus.CREATED).body(assembleiaservice.save(assembleiaModel));
	}
	@GetMapping(value = "/assembleialist")
	public ResponseEntity<List<AssembleiaModel>> getAssembleiaList(){
		List<AssembleiaModel> assembleia = assembleiaservice.getListAssembleia();
		
		if (assembleia.isEmpty()) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(assembleia);
			
		}
			return ResponseEntity.status(HttpStatus.OK).body(assembleia);
	}
}