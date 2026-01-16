package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import enums.Rank;
import enums.Suit;
import utils.EuchreComparator;

public class EuchreTable extends Table {
	private Suit trump;
	private List<Team> teams = new ArrayList<Team>();
	private Stack<Card> deck = new Stack<Card>();
	
	public void addTeam(Team t) {
		teams.add(t);
		players.addAll(t.getPlayers());
	}

	public void setTrump(Suit t) {
		trump = t;
		EuchreComparator.setTrump(t);
	}
	
	public void dealCards() {
		createAndShuffleDeck();
		int[] cardOptions = {3,2};
		int cardCounter = cardOptions[0];
		
		for (int dealPosition = dealerPosition+1; deck.size() > 4; dealPosition=(dealPosition+1) % players.size()) {
			Player currentPlayer = players.get(dealPosition);
			
			for(int j=0; j < cardCounter;j++) {
				currentPlayer.addCardToHand(deck.pop());
			}
			
			cardCounter = (dealPosition != 0) ? cardOptions[cardCounter == 2 ? 0 : 1] : cardCounter;
		}
	}
	
	private void createAndShuffleDeck() {
		for (Suit s : Suit.values()) {
			for (Rank r : Rank.values()) {
				deck.push(new Card(s,r));
			}
		}
		
		Collections.shuffle(deck);
	}
	
	public int compare(Card c1, Card c2) {
		if (isTrump(c1) && isTrump(c2)) {
			return EuchreComparator.compare(c1, c2);
		}
		else if (c1.getSuit() == trump && c2.getSuit() != trump) {
			return 1;
		}
		else if (c1.getSuit() != trump && c2.getSuit() == trump) {
			return -1;
		}
		else {
			return c1.getRank().compareTo(c2.getRank());
		}
	}
	
	public boolean isTrump(Card c) {
		Suit opposite = Suit.valueOf(c.getSuit().getOpposite());
		
		return c.getSuit() == trump ||
				(c.getRank() == Rank.JACK && opposite == trump);
	}

	public void chooseTrump() {
		Card potentialTrump = deck.pop();
		Player currentPlayer = players.get(dealerPosition+1);
		int playerChoice = (int) Math.floor(Math.random() * 2);
		
		System.out.println("choice:" + playerChoice);
		
		if (TrumpDecision.values()[playerChoice] == TrumpDecision.PICKUP) {
			
		}
	}
}

enum TrumpDecision {
	PASS,
	PICKUP;
}