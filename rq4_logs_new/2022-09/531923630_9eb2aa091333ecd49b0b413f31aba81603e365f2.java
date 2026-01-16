package com.bridgelabz;

public class Card {
    public static final String[] suitArray = new String[]{"Clubs", "Diamond","Heart","Spades"};
    public static final String[] rankArray = new String[]{"2", "3","4","5","6","7","8","9","Jack","Queen","king","Ace"};
    public static Card[] cardArray = new Card[52];

    private String suit;
    private String rank;

    public Card(String suit, String rank){
        this.suit = suit;
        this.rank = rank;
    }

    public String getSuit() {
        return suit;
    }

    public String getRank() {
        return rank;
    }

    public void setSuit(String suit) {
        this.suit = suit;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }
}