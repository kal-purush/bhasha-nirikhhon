package com.tramp.controlescolar.controllers;

import com.sendgrid.Request;
import com.sendgrid.Response;
import com.tramp.controlescolar.services.AlumnosService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/alumnos")
public class AlumnosController {
    @Autowired
    private AlumnosService alumnosService;

    @PostMapping("/insertaInscripcion")
    public ResponseEntity<?> insertarInscripcion(@RequestBody Map<String, Object> body){

        System.out.println("Datos recibidos: " + body);

        if (!body.containsKey("nid_usuario") || !body.containsKey("nid_grupo")) {
            return ResponseEntity.badRequest().body("Faltan parámetros en la petición");
        }

        Integer nid_usuario = Integer.valueOf(body.get("nid_usuario").toString());
        Integer nid_grupo = Integer.valueOf((body.get("nid_grupo").toString()));


        try{
            alumnosService.insertaInscripcion(nid_usuario, nid_usuario);

            return ResponseEntity.ok("Inscripción exitosa");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al insertar inscripción: " + e.getMessage());
        }

    }
    @GetMapping("/consultaAlumnosInscritos")
    public ResponseEntity<?> consultaAlumnosInscritos(
            @RequestParam Integer nid_grupo
    ){
        try{
            List<Map<String, Object>> alumnos = alumnosService.consultarAlumnosInscritos(nid_grupo);
            return ResponseEntity.ok(alumnos);
        }
        catch(Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al consultar horarios " + e.getMessage());

        }
    }

    @GetMapping("/consultaCalificaciones")
    public ResponseEntity<?> consultarCalificaciones(
            @RequestParam Integer nid_usuario
    ){
        try{
            List<Map<String, Object>> calificaciones = alumnosService.consultarCalificaciones(nid_usuario);
            return ResponseEntity.ok(calificaciones);
        }
        catch(Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al consultar calificaciones" + e.getMessage());

        }
    }

}