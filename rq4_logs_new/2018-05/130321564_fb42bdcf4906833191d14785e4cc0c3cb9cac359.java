package com.example.activiti.demo.controller;

import com.example.activiti.demo.model.Person;
import com.example.activiti.demo.servies.PersionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
@RequestMapping("person")
public class PersonController {
    @Autowired
    private PersionService persionService;

    @PostMapping
    public Person save() {
        Person person = new Person("zhanghong", new Date());
        return persionService.save(person);
    }

    @DeleteMapping("{id}")
    public void delete(@PathVariable(value = "id") Long id) {
        persionService.delete(id);
    }
}