package com.autentia.catalogocursos.springbootmybatis.controllers;

import com.autentia.catalogocursos.springbootmybatis.mappers.ProfesorMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("api/profesor")
@CrossOrigin(origins = "http://localhost:4200")
public class ProfesorController {

    private ProfesorMapper profesorMapper;

    public ProfesorController(ProfesorMapper profesorMapper) { this.profesorMapper = profesorMapper; }

    @GetMapping("allProfesores")
    public ResponseEntity<?> getAll() {
        log.info("Obteniendo profesores ...");
        return new ResponseEntity<>(profesorMapper.findAll(), HttpStatus.OK);
    }
}