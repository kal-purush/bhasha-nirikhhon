package pe.edu.upc.project_security_g06.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.project_security_g06.dtos.AlergiasDTO;
import pe.edu.upc.project_security_g06.entities.Alergias;
import pe.edu.upc.project_security_g06.servicesinterfaces.IdAlergiasService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/alergias")
public class AlergiasController {
    @Autowired
    private IdAlergiasService aS;

    @GetMapping
    public List<AlergiasDTO> listar() {
        return aS.list().stream().map(x->{
            ModelMapper m=new ModelMapper();
            return m.map(x,AlergiasDTO.class);
        }).collect(Collectors.toList());
    }
    @PostMapping
    public void insertar(@RequestBody AlergiasDTO dto){
        ModelMapper m=new ModelMapper();
        Alergias d=m.map(dto,Alergias.class);
        aS.insert(d);
    }
}