package pe.edu.upc.waba.controllers;


import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.waba.dtos.CursoDTO;
import pe.edu.upc.waba.entities.Curso;
import pe.edu.upc.waba.serviceinterfaces.ICursoService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/cursos")
public class CursoController {
    @Autowired
    private ICursoService cS;

    @GetMapping
    public List<CursoDTO> listar() {
        return cS.list() .stream() .map(y->{
            ModelMapper m = new ModelMapper();
            return m.map(y,CursoDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void Insert(@RequestBody CursoDTO dto){
        ModelMapper m= new ModelMapper();
        Curso cu = m.map(dto, Curso.class);
        cS.insert(cu);
    }

    @GetMapping("/{id}")
    public CursoDTO listarId(@PathVariable("id")Integer id){
        ModelMapper m= new ModelMapper();
        CursoDTO dto=m.map(cS.ListId(id), CursoDTO.class);
        return dto;
    }

    @PutMapping
    public void modificar(@RequestBody CursoDTO dto){
        ModelMapper m= new ModelMapper();
        Curso cu=m.map(dto, Curso.class);
        cS.update(cu);
    }

    @DeleteMapping("/{id}")
    public void eliminar(@PathVariable("id")Integer id){ cS.delete(id);}

    @GetMapping("/busquedas")
    public List<CursoDTO> buscarCurso(@RequestParam String curso){
        return cS.buscarCurso(curso).stream().map(x->{
            ModelMapper m= new ModelMapper();
            return m.map(x, CursoDTO.class);
        }).collect(Collectors.toList());
    }
}


