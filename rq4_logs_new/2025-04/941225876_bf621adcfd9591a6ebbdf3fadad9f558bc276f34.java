package com.tramp.controlescolar.controllers;
import com.tramp.controlescolar.models.catalogos.Carreras;
import com.tramp.controlescolar.repository.CarrerasRepository;
import org.springframework.beans.factory.annotation.Autowired;
import com.tramp.controlescolar.services.CarrerasService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("carreras")
public class CarrerasController {
    
    
    @Autowired
    private CarrerasService carrerasService;

    @Autowired
    private CarrerasRepository carrerasRepository;

    @GetMapping("mostrar")
    public List<Carreras> mostrarCarreras() {
        List<Carreras> listaCarreras = carrerasRepository.findAll();
        return listaCarreras;
    }

    @PostMapping("agregar")
    public ResponseEntity<String> crearCarrera(@RequestBody Carreras carreras) {
        try {
            carrerasService.guardarCarrera(carreras);
            return ResponseEntity.ok("Agregado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al registrar alumno: " + e.getMessage());
        }
    }

    @PutMapping("actualizar")
    public ResponseEntity<String> actualizarCarrera(@RequestBody Carreras carreras) {
        try {
            carrerasService.actualizCarreras(carreras);
            return ResponseEntity.ok("Actualizado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al actualizar carrera: " + e.getMessage());
        }
    }

    @DeleteMapping("/eliminar/{id}")
    public ResponseEntity<String> eliminarCarrera(@PathVariable Integer id) {
        try {
            carrerasService.eliminarCarrera(id);
            return ResponseEntity.ok("Eliminado correctamente");
        } catch (Exception e) {
            return  ResponseEntity.status(500).body("Error al eliminar carrera: " + e.getMessage());
        }
    }
}

