package com.example.mi_b_wizard;

import com.example.mi_b_wizard.Data.Deck;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;



import static org.junit.Assert.assertEquals;


public class DeckTest {

    Deck deck;


    @Before
    public void setUp(){
        deck = new Deck();
        deck.resetDeck();
    }

    @After
    public void tearDown(){
        deck.resetDeck();
    }


    //Test if ArrayList has 5 entries
    @Test
    public void getFiveCards(){
        assertEquals(5,deck.getCards(5).size());
    }

    //Test if 5 cards get removed from Deck
    @Test
    public void removeFiveCardsFromDeck(){
        System.out.println("Size of Deck: " + deck.getCard().size());
        System.out.println(deck.getCards(5));
        System.out.println("Size of Deck: " + deck.getCard().size());
    }

    //Test if resetDeck() method is working
    @Test
    public void removeFiveCardsAndResetDeck(){
        System.out.println("Size of Deck: " + deck.getCard().size());
        System.out.println(deck.getCards(5));
        System.out.println("Size of Deck: " + deck.getCard().size());
        System.out.println("----- Reset Deck -----");
        deck.resetDeck();
        System.out.println("Size of Deck: " + deck.getCard().size());
    }

}