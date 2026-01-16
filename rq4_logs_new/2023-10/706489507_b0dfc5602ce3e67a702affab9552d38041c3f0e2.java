
import java.util.Random;

import card.Card;
import card.Suit;

public class Deck {
	private Card[] cards = new Card[52];
	public Deck() {		//create and place the 52 different cards into cards[]
		setCard();
		
	}
	
	public void shuffle() {							//shuffle the cards
		long seed = System.currentTimeMillis();		//every games have different seed for random number
        Random random = new Random(seed);
        for (int i = 0; i < 52; i++) {
            int randNum = random.nextInt(52);
            Card tempCard = cards[randNum];
            cards[randNum] = cards[i];
            cards[i] = tempCard;
        }
	}
	
	public void setCard() {
		int index = 0;
		for (Suit suit : Suit.values()) { 
			for (int j = 0; j < 13; j++) {
				cards[index] = new Card(j + 1, suit);
				index++;
			}
		}
	}
	
	public Card[] getCards() {
		return cards;
	} 
}