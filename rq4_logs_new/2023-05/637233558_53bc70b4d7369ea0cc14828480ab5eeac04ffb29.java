package br.com.pedrorafante.dslist.controllers;

import br.com.pedrorafante.dslist.dto.GameMinDTO;
import br.com.pedrorafante.dslist.entities.Game;
import br.com.pedrorafante.dslist.repositories.GameRepository;
import br.com.pedrorafante.dslist.services.GameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
        @RequestMapping(value = "/games")
public class GameController {

    @Autowired
    private GameService gameService;
    @GetMapping
    public List<GameMinDTO> findAll(){
        return gameService.findAll().stream().map(GameMinDTO::new).toList();
    }
}