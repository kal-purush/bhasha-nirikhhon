package com.experisproject.experisproject.Controllers;

import com.experisproject.experisproject.Models.PersonService;
import com.experisproject.experisproject.Models.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/api")
public class UserController {

    @Autowired
    private PersonService personService;

    @CrossOrigin(origins = "localhost:3000")
    @RequestMapping(value = "/person")
    public Person getAll(){
        return personService.getAll().get(0);
    }

}