package pe.edu.upc.project_security_g06.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.project_security_g06.dtos.Contacto_EmergenciaDTO;
import pe.edu.upc.project_security_g06.entities.Contacto_Emergencia;
import pe.edu.upc.project_security_g06.servicesinterfaces.IContacEmergenciaService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/contactoEmergencia")
public class ContactoEmergenciaController {
    @Autowired
    private IContacEmergenciaService ceS;

    @GetMapping
    public List<Contacto_EmergenciaDTO> listar() {
        return ceS.list().stream().map(x -> {
            ModelMapper m = new ModelMapper();
            return m.map(x, Contacto_EmergenciaDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void insertar (@RequestBody Contacto_EmergenciaDTO dto) {
        ModelMapper m = new ModelMapper();
        Contacto_Emergencia d = m.map(dto, Contacto_Emergencia.class);
        ceS.insert(d);
    }

}