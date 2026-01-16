package com.cartas.jaktani.controller;

import com.cartas.jaktani.dto.TypeDto;
import com.cartas.jaktani.exceptions.ResourceNotFoundException;
import com.cartas.jaktani.service.TypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/api/type")
public class TypeController {
    @Autowired
    TypeService typeService;

    @GetMapping(path = "/id/{id}")
    public Object getTypeByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return typeService.getTypeByID(id);
    }

    @GetMapping(path = "/all")
    public Object getAllType() {
        return typeService.getAllTypes();
    }
	
	@PostMapping(path = "/add") 
	public Object addType(@RequestBody TypeDto type) { 
		return typeService.addType(type); 
	}

	@PostMapping(path = "/edit") 
	public Object editType(@RequestBody TypeDto type) { 
		return typeService.editType(type); 
	}

    @PostMapping(path = "/delete/{id}")
    public Object deleteTypeByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return typeService.deleteTypeByID(id);
    }

}