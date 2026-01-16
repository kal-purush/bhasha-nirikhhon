package pe.edu.upc.project_security_g06.controllers;


import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.project_security_g06.dtos.UbicacionsDTO;
import pe.edu.upc.project_security_g06.entities.Ubicacions;
import pe.edu.upc.project_security_g06.servicesinterfaces.idPostalService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/ubicaciones")
public class UbicacionsControllers {
    @Autowired
    private idPostalService pS;

    @GetMapping
    public List<UbicacionsDTO> listar() {
        return pS.list().stream().map(x->{
            ModelMapper m=new ModelMapper();
            return m.map(x,UbicacionsDTO.class);
        }).collect(Collectors.toList());
    }
    @PostMapping
    public void insertar(@RequestBody UbicacionsDTO dto){
        ModelMapper m=new ModelMapper();
        Ubicacions d=m.map(dto,Ubicacions.class);
        pS.insert(d);
    }

}