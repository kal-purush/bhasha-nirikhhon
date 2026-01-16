package com.filegenie.filegeniebackend.Controllers;

import com.filegenie.filegeniebackend.Entities.Field;
import com.filegenie.filegeniebackend.Repositories.FieldRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping(path="/field")
public class FieldController {
    @Autowired
    private FieldRepository fieldRepository;

    @PostMapping(path="/create")
    public @ResponseBody Long addNewField (
            @RequestParam String name,
            @RequestParam String value
    ) {
        Field f = new Field();
        f.setName(name);
        f.setValue(value);

        return f.getId();
    }

    @GetMapping(path="/all")
    public @ResponseBody Iterable<Field> getAllFields() {
        return fieldRepository.findAll();
    }

}