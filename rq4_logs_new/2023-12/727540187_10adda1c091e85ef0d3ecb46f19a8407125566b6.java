package com.team4.payroll.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.team4.payroll.dto.EmpleadoDTO;
import com.team4.payroll.service.EmpleadoService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

@Tag(name = "Endpoints de Empleados", description = "CRUD de empleados")
@RestController
@RequestMapping("empleados")
public class EmpleadoController {

    @Autowired
    private EmpleadoService service;

    @Operation(summary = "Obtener la lista de todos los empleados")
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<EmpleadoDTO> findAll() {
        return service.findAll();
    }
}