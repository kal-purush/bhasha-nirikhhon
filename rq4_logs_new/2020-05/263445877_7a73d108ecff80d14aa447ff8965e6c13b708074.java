package com.ashathor.rpggremlins.controllers;

import com.ashathor.rpggremlins.models.Ability;
import com.ashathor.rpggremlins.repositories.AbilityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/abilities")
public class AbilitiesController {

    @Autowired
    private AbilityRepository abilityRepository;

    @GetMapping
    public List<Ability> list(){
        return abilityRepository.findAll();
    }

    @GetMapping("/{index}")
    public Ability get(@PathVariable("index") String index){
        return abilityRepository.findByIndex(index);
    }
}