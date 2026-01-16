import java.util.ArrayList;

import card.Card;

public class Tableau{
	private int topIndex;
	private ArrayList<Card> cardList;
	
	public Tableau() {
		topIndex = -1;
		cardList = new ArrayList<Card>();
		
	}
	
	public boolean isEmpty(){
	    return cardList.isEmpty();
	}
	
	public boolean checkValidAction(Card card) {	//check if the card fulfil the requirement to push into tableau
		if(isEmpty()) {			//if tableau is empty, only 'K' rank can put
			if(card.getRank() == 13) {
				return true;
			}else{
				return false;
			}
		}
		else {					//if tableau is not empty, check whether the rank of (the last card - put card = 1) and have different color
			return checkValidCard(card, peek());
		}
		
	}
	
	//check correct rank and suit
	public boolean checkValidCard(Card moveFromCard, Card moveToCard) {
		if((moveToCard.getRank() - moveFromCard.getRank()) == 1 && (moveFromCard.getSuit().getColor() != moveToCard.getSuit().getColor())) {
			return true;
		}else{
			return false;
		}
	}
	
	//put card(s) from another Tableau
	public void push(ArrayList<Card> cards) {
		for(int i = cards.size()-1; i >= 0; i--) {
			cardList.add(cards.get(i));
			topIndex++;
		}
	}
	
	//remove last card
	public void pop() {
		cardList.remove(topIndex);
		topIndex--;
	}
	
	
	
//	public boolean checkPop(Card targetCard) {
//		if(topIndex < 0) {	//No card in tableau
//			return false;
//		}
//		int tempIndex = topIndex;
//		while(targetCard.getRank() > cardList.get(tempIndex).getRank()) {//Target card should bigger than "Move from" card(s)
//			if(tempIndex == 0) {	//Only 1 card in tableau
//				//if-else
//			}
//			
//			if((targetCard.getRank() - cardList.get(tempIndex).getRank()) == 1) {
//				
//			}
//		}
//		return
//	}
	

	public ArrayList<Card> getTableau() {
		return cardList;
    }
	
	public Card peek() {	//return the top card
		return cardList.get(topIndex);
	}
	

	
	
}
	