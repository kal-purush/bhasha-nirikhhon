package tech.munidigital.lavadero.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.munidigital.lavadero.dto.request.TurnoRequestDTO;
import tech.munidigital.lavadero.dto.response.TurnoResponseDTO;
import tech.munidigital.lavadero.service.TurnoService;
import java.util.List;

@RestController
@RequestMapping("/v1/api/turnos")
@RequiredArgsConstructor
public class TurnoController {

    private final TurnoService turnoService;

    // Crear un turno
    @PostMapping
    public ResponseEntity<TurnoResponseDTO> createTurno(@Valid @RequestBody TurnoRequestDTO turnoRequestDTO) {
        TurnoResponseDTO response = turnoService.createTurno(turnoRequestDTO);
        return new ResponseEntity<>(response, HttpStatus.CREATED);
    }

    // Obtener un turno por id
    @GetMapping("/{id}")
    public ResponseEntity<TurnoResponseDTO> getTurnoById(@PathVariable Long id) {
        TurnoResponseDTO response = turnoService.getTurnoById(id);
        return ResponseEntity.ok(response);
    }

    // Listar turnos asociados a un vehículo
    @GetMapping("/vehiculo/{vehiculoId}")
    public ResponseEntity<List<TurnoResponseDTO>> getTurnosByVehiculoId(@PathVariable Long vehiculoId) {
        List<TurnoResponseDTO> response = turnoService.getTurnosByVehiculoId(vehiculoId);
        return ResponseEntity.ok(response);
    }

}