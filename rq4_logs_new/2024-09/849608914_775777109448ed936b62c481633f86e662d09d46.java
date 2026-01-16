package pe.edu.upc.waba.controllers;


import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.waba.dtos.QuerysDTO.ReservaAsesoriaQ1DTO;
import pe.edu.upc.waba.dtos.QuerysDTO.ReservaAsesoriaQ2DTO;
import pe.edu.upc.waba.dtos.ReservaAsesoriaDTO;
import pe.edu.upc.waba.entities.ReservaAsesoria;
import pe.edu.upc.waba.serviceinterfaces.IReservaAsesoriaService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/reservaasesorias")
public class ReservaAsesoriaController {

    @Autowired
    private IReservaAsesoriaService raS;

    @GetMapping
    public List<ReservaAsesoriaDTO> listar() {
        return raS.list().stream().map(x->{
            ModelMapper m= new ModelMapper();
            return  m.map(x, ReservaAsesoriaDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void insertar(@RequestBody ReservaAsesoriaDTO dto){
        ModelMapper m=new ModelMapper();
        ReservaAsesoria rar=m.map(dto,ReservaAsesoria.class);
        raS.insert(rar);
    }

    @PutMapping
    public void modificar(@RequestBody ReservaAsesoriaDTO dto){
        ModelMapper m=new ModelMapper();
        ReservaAsesoria rar=m.map(dto,ReservaAsesoria.class);
        raS.update(rar);
    }

    @DeleteMapping("/{id}")
    public void eliminar(@PathVariable("id") Integer id){
        raS.delete(id);
    }


    @GetMapping("/cantidades")
    public List<ReservaAsesoriaQ1DTO> obtenerCantidad(){
        List<String[]>lista=raS.cantidad();
        List<ReservaAsesoriaQ1DTO>listaDTO=new ArrayList<>();
        for(String[] columna:lista){
            ReservaAsesoriaQ1DTO dto=new ReservaAsesoriaQ1DTO();
            dto.setModailidad(columna[0]);
            dto.setCantModalidad(Integer.parseInt(columna[1]));
            listaDTO.add(dto);
        }
        return listaDTO;
    }

    @GetMapping("/turnos")
    public List<ReservaAsesoriaQ2DTO> obtenerCantTurnos(){
        List<String[]>lista=raS.turno();
        List<ReservaAsesoriaQ2DTO>listaDTO=new ArrayList<>();
        for(String[] columna:lista){
            ReservaAsesoriaQ2DTO dto=new ReservaAsesoriaQ2DTO();
            dto.setTurno(columna[0]);
            dto.setCantTurno(Integer.parseInt(columna[1]));
            listaDTO.add(dto);
        }
        return listaDTO;
    }
}