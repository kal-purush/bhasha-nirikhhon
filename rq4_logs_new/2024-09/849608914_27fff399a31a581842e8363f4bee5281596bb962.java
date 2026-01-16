package pe.edu.upc.waba.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.waba.dtos.ComentarioDTO;

import pe.edu.upc.waba.entities.Comentario;

import pe.edu.upc.waba.serviceinterfaces.IComentarioService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/comentarios")
public class ComentarioController {
    @Autowired
    private IComentarioService cS;

    @GetMapping
    public List<ComentarioDTO> listar() {
        return cS.list() .stream() .map(y->{
            ModelMapper m = new ModelMapper();
            return m.map(y,ComentarioDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void Insert(@RequestBody ComentarioDTO dto){
        ModelMapper m= new ModelMapper();
        Comentario co = m.map(dto, Comentario.class);
        cS.insert(co);
    }

    @GetMapping("/{id}")
    public ComentarioDTO listarId(@PathVariable("id")Integer id){
        ModelMapper m= new ModelMapper();
        ComentarioDTO dto=m.map(cS.ListId(id), ComentarioDTO.class);
        return dto;
    }

    @PutMapping
    public void modificar(@RequestBody ComentarioDTO dto){
        ModelMapper m= new ModelMapper();
        Comentario co=m.map(dto, Comentario.class);
        cS.update(co);
    }

    @DeleteMapping("/{id}")
    public void eliminar(@PathVariable("id")Integer id){ cS.delete(id);}


}