package com.bridgelabz;

public class Cards {

	public static final String[] suitArray = new String[]{"Clubs", "Diamonds", "Hearts", "Spades"};
	public static final String[] rankArray = new String[]{"2", "3", "4", "5", "6", "7", "8","9", "10", "Jack", "Queen", "King", "Ace"};
	public static Cards[] cardArray = new Cards[52];

	private String suit;
	private String rank;


	public Cards(String suit, String rank) {
		this.suit = suit;
		this.rank = rank;
	}

	public String getSuit() {
		return suit;
	}

	public void setSuit(String suit) {
		this.suit = suit;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}
}
