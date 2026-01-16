package com.example.mi_b_wizard.Data;

public class Card {
    
    //Create 4 different card colours
    public enum Colour{
        BLAU(0),
        GRÜN(1),
        GELB(2),
        ROT(3);

        //variable to get the values
        private int value;

        //Enum constructor
        Colour(int value){
            this.value = value;
        }
    }

    //Enum for the different ranks of the cards
    public enum Rank{
        NARR(0),
        EINS(1),
        ZWEI(2),
        DREI(3),
        VIER(4),
        FÜNF(5),
        SECHS(6),
        SIEBEN(7),
        ACHT(8),
        ZAUBERER(9);

       private int value;

        Rank(int value) {
            this.value = value;
        }
    }
    private Rank rank;
    private Colour colour;
    private String id;

    //Constructor
    public Card(int rank, int colour){
        setRank(rank);
        setColour(colour);
        setId(rank, colour);
    }

    public Rank getRank(){
        return rank;
    }

    public void setRank(int rank){
        if(rank <0|| rank >9){
            throw new RuntimeException("Rank must be between 0 and 9");
        }else
        this.rank = Rank.values()[rank];
    }

    public Colour getColour(){
        return colour;
    }

    public void setColour(int colour){
        if(colour <0|| colour >3){
            throw new RuntimeException("Colour must be between 0 and 3");
        }else
        this.colour = Colour.values()[colour];
    }

    public boolean isMagician(){
        return this.rank == Rank.values()[9];
    }
    public boolean isNarr(){
        return this.rank ==Rank.values()[0];
    }
    //SetId example Rank= ZAUBERER, Colour = CRÜN -->ZAUBERER_GRÜN
    public void setId(int rank, int colour) {
        this.id = Rank.values()[rank].toString()+"_"+Colour.values()[colour].toString();
    }
    public String getId(){
        return id;
    }

    public boolean equalToOtherCard(Card otherCard){
        if (this.getColour() == otherCard.getColour() && this.getRank() == otherCard.getRank()){
            return true;
        }else
            return false;
    }

    public void showCard(){
        //needs to be implemented
        System.out.println(toString()+", ");
    }
    //This method returns e.g. "ZWEI in BLAU"
    @Override
    public String toString(){
        return String.format("%s in %s", getRank(),getColour());
    }




//Main Methode to test the functionality
   /* public static void main(String[] args) {
        Card cards;
        Card cards1;

        cards = new Card(9,1);
        System.out.println("Rank: "+cards.getRank()+ " Colour: "+ cards.getColour());
        System.out.println("Methode toString: "+cards.toString());
        System.out.println("Is card a Magican: "+ cards.isMagician());
        System.out.println("Is card a Narr: "+cards.isNarr());
        System.out.println("ID of the card: "+ cards.getId());

        cards1 = new Card(9, 1);
        System.out.println("Are these cards equal: "+cards1.equalToOtherCard(cards));
    }*/
}