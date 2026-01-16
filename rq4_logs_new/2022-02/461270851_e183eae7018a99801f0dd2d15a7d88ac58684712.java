package blackjack;

public class Card {
	private cardSuits suit ;
	private cardNumbers number ;
	
	//Constructor
	public Card ( cardSuits suit , cardNumbers number ) {
		this.suit = suit ;
		this.number = number ;
	}
	
	public cardNumbers getNumber() {
		return number ;
	}
	public cardSuits getSuit() {
		return suit;
	}
	
	@Override
	public String toString() {
		return number.toString() + " of " + suit.toString();
	}


}
