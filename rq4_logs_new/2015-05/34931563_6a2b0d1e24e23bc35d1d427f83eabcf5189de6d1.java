import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by kegan on 5/2/15.
 */
public class Deck {
    ArrayList<Card> deck = new ArrayList<Card>();
    ArrayList<Card> discardPile = new ArrayList<Card>();
    public Deck(){
        for (int i = 1; i <= 13; i++) {
            deck.add(new Card(i, 1));
            deck.add(new Card(i, 2));
            deck.add(new Card(i, 3));
            deck.add(new Card(i, 4));
        }
    }
    public void printDeck(){
        for (Card crd : deck){
            System.out.println(crd.getRank() + " of " + crd.getSuit());
        }
    }
    public void shuffle(){
        Collections.shuffle(deck);
    }
    public Card draw(){
        return deck.remove(0);
    }
    public void discard(Card crd){
        discardPile.add(crd);
    }
    public void resetDeck(){
        deck.addAll(discardPile);
        discardPile.clear();
    }
}