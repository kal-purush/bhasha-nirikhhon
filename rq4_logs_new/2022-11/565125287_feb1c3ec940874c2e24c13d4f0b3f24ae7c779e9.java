package com.demo.pdf.gen.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.pdf.gen.service.BlobCreation;

@Controller
@RequestMapping("/api/tree")
public class PdfController {
	@Autowired
	BlobCreation blob;
	
//	PdfController(EmployeeRepository repository) {
//	    this.repository = repository;
//	  }

//	@GetMapping("/{id}")
//	public ResponseEntity<?> getBazz(@PathVariable String id){
//	    return new ResponseEntity<>(new Bazz(id, "Bazz"+id), HttpStatus.OK);
//	}
	
	@GetMapping("/blob")
	public String getBazz(){
		String r=blob.createBlob();
	    return r;
	}
}