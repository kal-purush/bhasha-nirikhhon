package pe.edu.upc.project_security_g06.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.project_security_g06.entities.Dispositivo;
import pe.edu.upc.project_security_g06.dtos.DispositivoDTO;
import pe.edu.upc.project_security_g06.servicesinterfaces.IDispositivoService;


@RestController
@RequestMapping("/dispositivos")
@PreAuthorize("hasAnyAuthority('DISPOSITIVO')")
public class DispositivoController {
    @Autowired
    private IDispositivoService dS;

    @PostMapping
    public void insertar(@RequestBody DispositivoDTO dto) {
        ModelMapper m = new ModelMapper();
        Dispositivo dis = m.map(dto, Dispositivo.class);
        dS.insert(dis);
    }

    @GetMapping("/{id}")
    public DispositivoDTO listarId(@PathVariable("id") Integer id) {
        ModelMapper m = new ModelMapper();
        DispositivoDTO dto = m.map(dS.list(id), DispositivoDTO.class);
        return dto;
    }
}