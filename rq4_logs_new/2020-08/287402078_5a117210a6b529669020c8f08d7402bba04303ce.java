package com.minesweeper.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.minesweeper.api.domain.Game;
import com.minesweeper.api.service.GameService;

@Component
public class AppStartupRunner implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(AppStartupRunner.class);

    @Autowired
    private GameService gameService;
   

    @Override
    public void run(ApplicationArguments args) throws Exception {
    	//Insert one first game for testing purpose
        Game newGame = new Game();
        gameService.save(newGame);
        LOG.info("Insert Game succesfully: {}", newGame.getName());
    }

}