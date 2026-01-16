package pe.edu.upc.project_security_g06.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.project_security_g06.dtos.Contacto_AutoridadesDTO;
import pe.edu.upc.project_security_g06.dtos.Contacto_EmergenciaDTO;
import pe.edu.upc.project_security_g06.entities.Contacto_Autoridades;
import pe.edu.upc.project_security_g06.entities.Contacto_Emergencia;
import pe.edu.upc.project_security_g06.servicesinterfaces.IContacAutoridadesService;
import pe.edu.upc.project_security_g06.servicesinterfaces.IContacEmergenciaService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/contactosAutoridades")
public class ContactoAutoridadesController {
    @Autowired
    private IContacAutoridadesService caS;

    @GetMapping
    public List<Contacto_AutoridadesDTO> listar() {
        return caS.list().stream().map(x -> {
            ModelMapper m = new ModelMapper();
            return m.map(x, Contacto_AutoridadesDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void insertar (@RequestBody Contacto_AutoridadesDTO dto) {
        ModelMapper m = new ModelMapper();
        Contacto_Autoridades d = m.map(dto, Contacto_Autoridades.class);
        caS.insert(d);
    }

}