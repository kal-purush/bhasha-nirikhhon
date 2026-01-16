package com.tramp.controlescolar.controllers;

import com.tramp.controlescolar.services.HorariosService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/horarios")
public class HorariosController {
    @Autowired
    private HorariosService horariosService;

    @GetMapping("/consulta")
    public ResponseEntity<?> consultaHorario(
            @RequestParam Integer carrera,
            @RequestParam Integer grupo
    ) {
        try {
            List<Map<String, Object>> horarios = horariosService.obtenerHorario(carrera, grupo);
            return ResponseEntity.ok(horarios);
        }
        catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al consultar horarios " + e.getMessage());
        }
    }

}