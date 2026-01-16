package com.example.mi_b_wizard.Data;

import java.util.ArrayList;
import java.util.List;

public class Player{

    private String playerName;
    private int points;
    private int madeTrick; // tricks, which were made (gemachte Stiche)
    private int predictedTrick; //tricks, which were predicted (angesagte Stiche)
    private List<Card> hand;  //Cards in the hand of a player

    public Player(String playerName){
        setPlayerName(playerName);
        hand = new ArrayList<>();
    }

    //Getter-Setter

    public void setPlayerName(String name){
        this.playerName = playerName;
    }

    public String getPlayerName() {
        return playerName;
    }

    public int getMadeTrick() {
        return madeTrick;
    }

    public void setMadeTrick(int madeTrick) {
        this.madeTrick = madeTrick;
    }

    public int getPredictedTrick() {
        return predictedTrick;
    }

    public void setPredictedTrick(int predictedTrick) {
        this.predictedTrick = predictedTrick;
    }

    public List<Card> getHand() {
        return hand;
    }

    public void setHand(List<Card> hand) {
        this.hand = hand;
    }

    public int getPoints() {
        return points;
    }

    public void setPoints(int points) {
        this.points = points;
    }

    //<-----PlayerHand----->


    //adds a card to the hand
    public void addCardToHand(Card card){
        hand.add(card);
    }
    // method to show all cards in the hand
    public void showHand(){
        for(Card card : hand){
            card.showCard();
        }
    }
    public void clearHand(){
        hand.clear();
    }
    public int getHandSize(){
        return hand.size();
    }
    public void removeCardFromHand(Card playedcard){
        hand.remove(playedcard);
    }


    //<-------------------------->

    //Needs to be updated (Game)
    public void updatePoints(){
        //points =+ Game.calculatePointsForOnePlayer(predictedTrick, madeTrick);
    }
    public void getActualCardsHandFromGame(){
        //needs to be updated
    }

    //<------Action Player------>(needs to be implemented)

    //Which card should be played
    public void playCard(Card card){

    }


    //Main Methode to test the functionality
    /*public static void main(String[] args) {

        List<Card> listHand = new ArrayList();
        Player player = new Player("Julia");
        Card c1 = new Card(0,1);
        player.addCardToHand(c1);
        player.addCardToHand(new Card(2,2));
        player.addCardToHand(new Card(3,3));
        player.showHand();
        System.out.println(player.getHandSize());
        player.removeCardFromHand(c1);
        System.out.println(player.getHandSize());
    }*/
}