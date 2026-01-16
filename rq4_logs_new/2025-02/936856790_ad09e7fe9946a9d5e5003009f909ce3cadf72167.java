package tech.munidigital.lavadero.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.munidigital.lavadero.dto.request.VehiculoRequestDTO;
import tech.munidigital.lavadero.dto.response.VehiculoResponseDTO;
import tech.munidigital.lavadero.service.VehiculoService;
import java.util.List;

@RestController
@RequestMapping("/v1/api/vehiculos")
@RequiredArgsConstructor
public class VehiculoController {

    private final VehiculoService vehiculoService;

    // Crear un vehículo
    @PostMapping
    public ResponseEntity<VehiculoResponseDTO> createVehiculo(@Valid @RequestBody VehiculoRequestDTO vehiculoRequestDTO) {
        VehiculoResponseDTO response = vehiculoService.createVehiculo(vehiculoRequestDTO);
        return new ResponseEntity<>(response, HttpStatus.CREATED);
    }

    // Obtener un vehículo por id
    @GetMapping("/{id}")
    public ResponseEntity<VehiculoResponseDTO> getVehiculoById(@PathVariable Long id) {
        VehiculoResponseDTO response = vehiculoService.getVehiculoById(id);
        return ResponseEntity.ok(response);
    }

    // Listar vehículos de un cliente
    @GetMapping("/cliente/{clienteId}")
    public ResponseEntity<List<VehiculoResponseDTO>> getVehiculosByClienteId(@PathVariable Long clienteId) {
        List<VehiculoResponseDTO> response = vehiculoService.getVehiculosByClienteId(clienteId);
        return ResponseEntity.ok(response);
    }

}