package tech.munidigital.lavadero.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.munidigital.lavadero.dto.request.CobroRequestDTO;
import tech.munidigital.lavadero.dto.response.CobroResponseDTO;
import tech.munidigital.lavadero.service.CobroService;

@RestController
@RequestMapping("/v1/api/cobros")
@RequiredArgsConstructor
public class CobroController {

    private final CobroService cobroService;

    // Endpoint para crear un cobro
    @PostMapping
    public ResponseEntity<CobroResponseDTO> createCobro(@Valid @RequestBody CobroRequestDTO cobroRequestDTO) {
        CobroResponseDTO response = cobroService.createCobro(cobroRequestDTO);
        return new ResponseEntity<>(response, HttpStatus.CREATED);
    }

}