package com.proyecto.integrador.hotel.libertador.controllers;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.proyecto.integrador.hotel.libertador.models.service.IS3Service;

@RestController
public class S3Controller {
	
	@Autowired
	private IS3Service s3Service;
	
	@PostMapping("/upload")
	public String uploadFile(@RequestParam("file") MultipartFile file) throws IOException {
		return s3Service.uploadFile(file);
	}
	
	@GetMapping("/download/{fileName}")
	public String downloadFile(@PathVariable("fileName") String fileName) throws IOException {
		return s3Service.downloadFile(fileName);
	}
}