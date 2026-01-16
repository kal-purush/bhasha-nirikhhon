package com.example.mi_b_wizard;

import com.example.mi_b_wizard.Data.Card;
import com.example.mi_b_wizard.Data.Player;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class PlayerTest {
    Player player;
    Player player2;
    @Before
    public void before(){
        player =  new Player("Tom");
        player2 = new Player("Lisa");
    }
    @After
    public void after(){
        player = null;
    }

    @Test
    public void testGetName(){
        assertEquals("Tom",player.getPlayerName());
        assertEquals("Lisa", player2.getPlayerName());
    }
    @Test
    public void testSetName(){
        assertEquals("Tom", player.getPlayerName());
        player.setPlayerName("Max");
        assertEquals("Max", player.getPlayerName());
    }
    @Test
    public void testSetMadeTrick(){
        player.setMadeTrick(3);
        assertEquals(3, player.getMadeTrick());
        player.setMadeTrick(5);
        assertEquals(5, player.getPredictedTrick());
    }
    @Test
    public void testGetPoints(){
        assertEquals(0, player.getPoints());
    }
    @Test
    public void testSetPoints(){
        player.setPoints(5);
        assertEquals(5, player.getPoints());
    }
    @Test
    public void testSetActualPlayedCard(){
        assertEquals(null, player.getActualPlayedCard());
        Card card = new Card(0,0);
        player.setActualPlayedCard(card);
        assertEquals(card, player.getActualPlayedCard());
    }
    @Test
    public void testResetForNewRound(){
        player.setActualPlayedCard(new Card(1,2));
        player.setPredictedTrick(5);
        player.setMadeTrick(2);
        player.resetForNewRound();

        assertEquals(null, player.getActualPlayedCard());
        assertEquals(0, player.getPredictedTrick());
        assertEquals(0, player.getMadeTrick());
    }
    @Test
    public void testUpdatePoints(){
        player.updatePoints(5,2);
        assertEquals(-30, player.getPoints());
        player.updatePoints(3,3);
        assertEquals(20,player.getPoints());
    }
    @Test
    public void testPlayCard(){
        Card card = new Card(1,2);
        player.playCard(card);
        assertEquals(card, player.getActualPlayedCard());
    }
    @Test
    public void testUpdatePredictedtricks(){
        player.updatePredictedTricks();
        assertEquals(2, player.getPredictedTrick());
    }
}