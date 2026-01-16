package final330;

public class Blackjack {

	public static void main(String[] args) {
		System.out.println("Welcome to our Blackhack Game!");
		Deck playingDeck = new Deck();
		playingDeck.createFullDeck();
		System.out.println(playingDeck);
		playingDeck.shuffle();
		System.out.println("Deck Was Shuffled");

		System.out.println(playingDeck);
		Deck playerDeck = new Deck();
		Deck dealerDeck = new Deck();

	}
	
}