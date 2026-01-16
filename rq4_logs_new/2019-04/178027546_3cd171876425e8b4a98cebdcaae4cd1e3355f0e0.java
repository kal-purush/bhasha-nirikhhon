package com.example.mi_b_wizard.Data;


import java.util.HashMap;

import java.util.Set;

public class Hand {
    public HashMap<String,Card> hand;

    public Hand() {
        hand = new HashMap<>();
    }

    //add a list of cards to the hand
    public void addListOfCardsToHand(HashMap<String, Card> cards){
        hand.putAll(cards);
    }

    //adds a card to the hand
    public void addCardToHand(String id, Card card){
        hand.put(id,card);
    }

    //show all cards in the hand
    public void showHand(){
        Set keys = hand.keySet();
        for(Object key: keys){
            System.out.println(key +": "+ hand.get(key));
        }
        //Methode only print the keys
        /*System.out.println("Keys: "+keys);*/
    }
    public void clearHand(){
        hand.clear();
    }
    public int getHandSize(){
        return hand.size();
    }
    public void removeCardFromHand(Card playedcard){
        hand.remove(playedcard.getId());
    }
    public void setHand(){
        this.hand = hand;
    }
    public HashMap getHand(){
        return hand;
    }

    //Main Methode for testing the functionality
    /*public static void main(String[] args) {
        HashMap<String, Card> hand1 = new HashMap<>();
        Card card1 = new Card(0,1); Card card2 = new Card(1,2); Card card3 = new Card(2,3);Card card4 = new Card(2,2);
        hand1.put(card1.getId(),card1); hand1.put(card2.getId(),card2); hand1.put(card3.getId(),card3);hand1.put(card4.getId(),card4);

        Hand handClass = new Hand();
        handClass.addListOfCardsToHand(hand1);
        handClass.showHand();
        handClass.removeCardFromHand(card1);
        handClass.showHand();
    }*/
}