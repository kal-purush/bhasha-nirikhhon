package me.wup.gamesurvey.controllers;

import me.wup.gamesurvey.domain.dto.GameDto;
import me.wup.gamesurvey.services.GameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/games")
public class GameController {

    @Autowired
    private GameService gameService;

    @GetMapping()
    public ResponseEntity <List<GameDto>> findAllGames () {
        List<GameDto> games = gameService.findAllGames();
         return ResponseEntity.ok().body(games);
    }
}