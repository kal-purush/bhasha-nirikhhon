package org.vashonsd.Games.SP;

import org.vashonsd.Utils.Cards.Card;
import org.vashonsd.Utils.Cards.Hand;
import java.util.ArrayList;

public class Player {

    private Hand hand;
    private int cash =100;

    ArrayList<Card> hands;
    private int handvalue = 0;
    private Card[] aHand;

    public int getCash() {
        return cash;
    }

    public void resetCash() { cash = 100; }

    public void setCash(int cash) {
        this.cash = cash;
    }

    public Hand getHand() {
        return hand;
    }

    public void setHand(Hand hand) {
        this.hand = hand;
    }

    public Player() {
        hand = new Hand(5, 2);
    }

    public int getHandCapacity() {
        return this.hand.getCapacity();
    }

    public void takeCard(Card deal) {
        hand.takeCard(deal);
    }

    public int getHandStartingSize() {
        return hand.getStartingSize();
    }

   /* public static int calcHandValue(List<Card> hands) {
       int handvalue = 0;
        for (int i = 0; i < hands.size(); i++) {
            handvalue += hands.get(i).getValue();
        }
        return handvalue;
    }*/
}