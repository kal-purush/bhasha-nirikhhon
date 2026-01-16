package solitaire;

import java.awt.Graphics;
import java.awt.Point;
import java.util.ArrayList;

public class CardPile {

	protected ArrayList<Card> cards;
	private int x=0;		//x position
	private int y=0;		//y position
	
	public CardPile() {
		cards = new ArrayList<Card>();
	}
	//put the new card into the end of ArrayList
	public void push(Card aCard) {
		cards.add(aCard);
	}
	//get the card from the end of ArrayList and remove it
	public Card pop() {
		if (cards.isEmpty()) return null;
		return cards.remove(cards.size()-1);
	}
	//get the card at the end of the ArrayList
	public Card top() {
		if (cards.isEmpty()) return null;
		return cards.get(cards.size()-1);
	}
	//get the card at the beginning of ArrayList
	public Card tail() {
		if (cards.isEmpty()) return null;
		return cards.get(0);
	}
	//get how many cards in the pile
	public int getActualSize() {
		return cards==null?0:cards.size();
	}
	//set position of card pile
	public void setPosition(int x, int y) {
		this.x = x;
		this.y = y;
	}
	//get position of card pile
	public Point getPoint(int x,int y) {
		return new Point(x,y);
	}
	//draw the card pile
	public void draw(Graphics g) {
		
	}
}