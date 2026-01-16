package pe.edu.upc.waba.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pe.edu.upc.waba.dtos.TasesoriaDTO;
import pe.edu.upc.waba.entities.Tasesoria;
import pe.edu.upc.waba.serviceinterfaces.ITasesoriaService;


import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/tasesorias")
public class TasesoriaController {

    @Autowired
    private ITasesoriaService taS;

    @GetMapping
    public List<TasesoriaDTO> listar() {
        return taS.list().stream().map(y -> {
            ModelMapper m = new ModelMapper();
            return m.map(y, TasesoriaDTO.class);
        }).collect(Collectors.toList());
    }

    @PostMapping
    public void insert(@RequestBody TasesoriaDTO dto) {
        ModelMapper m = new ModelMapper();
        Tasesoria a = m.map(dto, Tasesoria.class);
        taS.insert(a);
    }

    @GetMapping("/{id}")
    public TasesoriaDTO listarId(@PathVariable("id") Integer id) {
        ModelMapper m = new ModelMapper();
        return m.map(taS.listId(id), TasesoriaDTO.class);
    }

    @PutMapping
    public void modificar(@RequestBody TasesoriaDTO dto) {
        ModelMapper m = new ModelMapper();
        Tasesoria a = m.map(dto, Tasesoria.class);
        taS.update(a);
    }

    @DeleteMapping("/{id}")
    public void eliminar(@PathVariable("id") Integer id) {
        taS.delete(id);
    }
}