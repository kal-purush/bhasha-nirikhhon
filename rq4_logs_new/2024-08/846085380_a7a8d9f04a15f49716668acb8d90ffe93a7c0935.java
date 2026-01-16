package vn.hust.hedspi.ezsport.controllers;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import vn.hust.hedspi.ezsport.dtos.CreateFieldRequest;
import vn.hust.hedspi.ezsport.entities.Field;
import vn.hust.hedspi.ezsport.repositories.FieldRepository;

import java.util.UUID;

@RestController()
@RequestMapping("api/v1/field")
@NoArgsConstructor
@AllArgsConstructor
public class FieldController {
    @Autowired
    private FieldRepository fieldRepository;

    @GetMapping()
    public Object getFieldInfo(@RequestParam String id){
        return fieldRepository.getReferenceById(id);
    }

    @PostMapping()
    public Object createField(@RequestBody CreateFieldRequest requestBody){
        Field field = new Field();
        field.setName(requestBody.getName());
        field.setDescription(requestBody.getDescription());
        field.setLongitude(requestBody.getLongitude());
        field.setLatitude(requestBody.getLatitude());

        return fieldRepository.save(field);
    }
}