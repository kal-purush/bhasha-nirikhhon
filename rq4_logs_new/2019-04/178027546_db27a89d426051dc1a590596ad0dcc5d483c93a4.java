package com.example.mi_b_wizard.Data;


import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


public class Deck {

    List<Card> deck;



    private void decksinit() {
        deck = new LinkedList<>();
    }

    /*

    The Game contains 40 cards
    4 colours each with numbers from 1-8
        - blue
        - green
        - yellow
        - red

    4 Wizards
    4 Narren

*/


    public void deck_ready() {

        createCards();

        shuffle();
        System.out.println(this.deck.size() + " Cards are ready in Deck.");

    }

    /*
     Create new Card

     -> create CardCount to hold how many cards have been created
     -> create as many Cards as there are game rounds
     -> create color and number, add the Card into the List

     */


    //Create Cards
    private void createCards() {
        int iCardCount = 0;


        clearDeck();
        while (iCardCount <= countRound) {                //Game Round Counter needs to be implemented
            for (int color = 0; color < 4; color++) {
                for (int number = 0; number < 8; number++) {
                    deck.add(new Card(color, number));
                    iCardCount++;
                }
            }
        }
    }



    public void shuffle(){
        Collections.shuffle(this.deck);
    }



    public int deckSize(){
        return deck.size();
    }



    //to clear Deck for next Round
    public void clearDeck(){
        deck.clear();
    }



    @Override
    public String toString(){
        return "Deck{" + "cardDeck=" + deck + "}";
    }



    //getter and setter
    public List<Card> getDeck() {
        return deck;
    }

    public void setDeck(List<Card> deck) {
        this.deck = deck;
    }
}



