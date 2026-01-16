package ru.clean.process.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.clean.process.db.Actor;
import ru.clean.process.db.ActorRepository;

import java.util.List;


@RestController
@RequestMapping(value = "/movie")
public class MovieController {

    private final ActorRepository actorRepository;

    @Autowired
    public MovieController(ActorRepository actorRepository) {
        this.actorRepository = actorRepository;
    }

    @RequestMapping(value = "/actors")
    public List<Actor> getAllActors() {
        return actorRepository.findAll();
    }
}