package com.tramp.controlescolar.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import com.tramp.controlescolar.dto.PersonaUsuarioRequest;
import com.tramp.controlescolar.services.PersonasService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;


@RestController
@CrossOrigin(origins = "*")
@RequestMapping("registrar")
public class PersonasController {

    @Autowired
    private PersonasService personasService;

    //Ingresa al procedimiento almacenado SPD_INSERTA_PERSONA_ADMIN
    @PostMapping("admnistrador")
    public ResponseEntity<String> crearAdministrador(@RequestBody PersonaUsuarioRequest request){
        try {
            personasService.insertarAdministrador(request);
            return ResponseEntity.ok("Admnistrador registrado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al registrar administrador: " + e.getMessage());

        }
    }

    //Ingresa al procedimiento almacenado SPD_INSERTA_PERSONA_ALUMNO
    @PostMapping("profesor")
    public ResponseEntity<String> crearProfesor(@RequestBody PersonaUsuarioRequest request){
        try {
            personasService.insertarProfesor(request);
            return ResponseEntity.ok("Profesor registrado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al registrar profesor: " + e.getMessage());

        }
    }

    @PostMapping("alumno")
    public ResponseEntity<String> crearAlumno(@RequestBody PersonaUsuarioRequest request){
        try {
            personasService.insertarAlumno(request);
            return ResponseEntity.ok("Alumno registrado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al registrar alumno: " + e.getMessage());

        }
    }

    
}