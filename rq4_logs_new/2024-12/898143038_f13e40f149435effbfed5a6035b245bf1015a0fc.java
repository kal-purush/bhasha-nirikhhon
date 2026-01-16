package nl.novi.techiteasy.controllers;

import nl.novi.techiteasy.dtos.CIModuleDTO;
import nl.novi.techiteasy.services.CIModuleService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/v1/cimodules")
public class CIModuleController {
    private final CIModuleService ciModuleService;

    public CIModuleController(CIModuleService ciModuleService){
        this.ciModuleService = ciModuleService;
    }

    @GetMapping
    public ResponseEntity<List<CIModuleDTO>> getAllCIModules() {
        List<CIModuleDTO> dtos = ciModuleService.getAllCIModules();
        return ResponseEntity.ok(dtos);
    }

    @GetMapping("/{id}")
    public ResponseEntity<CIModuleDTO> getCIModule(@PathVariable Long id) {
        CIModuleDTO ciModuleDTO = ciModuleService.getCIModuleById(id);
        return ResponseEntity.ok(ciModuleDTO);
    }

    @PostMapping
    public ResponseEntity<CIModuleDTO> addCIModule(@RequestBody CIModuleDTO dto) {
        CIModuleDTO ciModuleDTO = ciModuleService.saveCIModule(dto);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(ciModuleDTO.getId())
                .toUri();
        return ResponseEntity.created(location).body(ciModuleDTO);

    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteCIModule(@PathVariable Long id) {
        ciModuleService.deleteCIModuleById(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{id}")
    public ResponseEntity<CIModuleDTO> updateCIModule(@PathVariable Long id, @RequestBody CIModuleDTO ciModuleDTO) {
        CIModuleDTO updatedCIModuleDTO = ciModuleService.updateCIModule(id, ciModuleDTO);
        return ResponseEntity.ok(updatedCIModuleDTO);
    }

}