package com.commercetools.pingpong.controller;

import com.commercetools.pingpong.model.Game;
import com.commercetools.pingpong.model.Player;

import org.junit.Test;
import org.springframework.web.bind.annotation.RequestMapping;

import static org.junit.Assert.*;

public class GameControllerTest {

    @Test
    public void shouldReturnBubbleMessage() {
        Player playerTestOne = new Player("Bob");

        playerTestOne.setBeginningServe();

        Player playerTestTwo = new Player("Kelso");

        Game matchTest = new Game(playerTestOne, playerTestTwo);
        GameController gameControllerTest = new GameController();

    }
}
